"""Dispatcher: the Coordinator's work slots, launches and the routing of run results.

Every work run (agenda, queued human / kanban orders, resumes, takeover releases) shares
`work_cap` slots, so respond always keeps a claude slot. One backlog item has at most one
live run: an interrupted run with an open Ask keeps its item until the Ask closes.
"""

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Protocol

from swarm.aio import TaskSet
from swarm.backoff import Ladder
from swarm.clock import Clock
from swarm.coordinator.agenda import AgendaChoice
from swarm.coordinator.records import CoordinatorRecords
from swarm.coordinator.voice import Voice
from swarm.ids import new_id
from swarm.inbox.inbox import Inbox
from swarm.projects.store import ProjectStore
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.codec import to_json
from swarm.store.events import Emitter
from swarm.text import error_text, first_line
from swarm.types import (
    AskOutcome,
    AskView,
    ChannelRef,
    EngineId,
    ProjectId,
    QuotaLease,
    RunId,
    RunResult,
    SteerMode,
    TakeoverInfo,
    Variant,
    WorkerRun,
    WorkOrder,
)
from swarm.workers.contract import (
    FINISHED,
    STUCK_REASON,
    InvalidOrder,
    cancelled_summary,
    is_quota_pause,
    order_from_json,
    validate_order,
)

# work runs (agenda, queued human / kanban orders, resumes, takeover releases) use at most 2
# of claude's 3 slots; the third is always free for respond (Brain triage, answer mapping,
# steers), so a human is never locked out by our own work
WORK_CAP = 2
# a run that ends interrupted for anything but a quota pause (browser cannot start,
# verifier down) puts its item on hold before it may run again; the 3rd such ending in a row
# blocks the item and tells the human once. Reset when the item's run is done.
ITEM_BACKOFF = Ladder((60, 300))  # hold after the 1st, 2nd consecutive interrupt
ITEM_STOP_AFTER = len(ITEM_BACKOFF.steps) + 1  # this many in a row block the item

FYI_INTENT_MAX = 80  # an order's first line in an FYI
FAILURE_WHAT_MAX = 200
SUMMARY_NOTE_MAX = 300  # a run summary kept in a hold note
FYI_SUMMARY_MAX = 200  # a run summary in an FYI
BACKLOG_NOTE_MAX = 200
_ITEM_AFTER = {
    "done": "done",
    "failed": "blocked",
    "waiting_human": "blocked",
    "interrupted": "ready",
    "cancelled": "blocked",  # an archived (dropped) item stays dropped: item_status guards
}


class RunControl(Protocol):
    """The slice of `WorkerPool` the Coordinator drives runs through: start, resume, steer,
    take over, release and cancel, plus the three reads it plans with. The front declares its
    own slices (`front.internal_api.WorkerEndpoints` for a running worker's calls,
    `front.kanban.RunReader` for the board); `WorkerPool` satisfies all three."""

    async def run(
        self, order: WorkOrder, variant: Variant, *, lease: QuotaLease | None
    ) -> RunResult: ...
    async def resume(
        self, run_id: RunId, *, outcome: AskOutcome, lease: QuotaLease | None
    ) -> RunResult: ...
    async def wait(self, run_id: RunId) -> RunResult: ...
    async def steer(self, run_id: RunId, *, text: str) -> SteerMode: ...
    async def takeover(self, run_id: RunId) -> TakeoverInfo: ...
    async def release(self, run_id: RunId, *, note: str) -> None: ...
    async def cancel(self, run_id: RunId, *, reason: str) -> None: ...
    def running(self) -> list[WorkerRun]: ...
    def run_project(self, run_id: RunId) -> ProjectId | None: ...
    def get(self, run_id: RunId) -> WorkerRun: ...


class Dispatcher:
    def __init__(
        self,
        *,
        records: CoordinatorRecords,
        pool: RunControl,
        ledger: QuotaLedger,
        projects: ProjectStore,
        inbox: Inbox,
        voice: Voice,
        emit: Emitter,
        tasks: TaskSet,
        clock: Clock,
        wake: Callable[[], None],
        default_variant: Variant,
        work_cap: int,
        on_discovered: Callable[[WorkOrder, RunResult], Awaitable[None]],
    ) -> None:
        self._records = records
        self._pool = pool
        self._ledger = ledger
        self._projects = projects
        self._inbox = inbox
        self._voice = voice
        self._emit = emit
        self._tasks = tasks
        self._clock = clock
        self._wake = wake
        self._default_variant = default_variant
        self.engine: EngineId = default_variant["engine"]  # type: ignore[assignment]
        self._work_cap = work_cap
        self._on_discovered = on_discovered
        self._dispatching = 0
        self._recovered = False

    # --- work slots ------------------------------------------------------------------

    def running_count(self) -> int:
        busy = sum(1 for r in self._pool.running() if r["status"] != "waiting_human")
        return max(busy, self._dispatching)

    def work_full(self) -> bool:
        return self.running_count() >= self._work_cap

    def reserve_slot(self) -> Callable[[], None]:
        """Count one drive against the work cap until the returned release is called (once;
        later calls do nothing)."""
        self._dispatching += 1
        released = False

        def release() -> None:
            nonlocal released
            if not released:
                released = True
                self._dispatching -= 1

        return release

    # --- launching -------------------------------------------------------------------

    def variant_of(self, project_id: ProjectId | None) -> Variant:
        if project_id:
            return self._projects.get(project_id)["default_variant"]
        return self._default_variant

    def dispatch_queued(self) -> int:
        n = 0
        for r in self._records.orders_in("queued"):
            order = order_from_json(r["order_json"])
            item = order["backlog_item_id"]
            backlog = self._projects.get(order["project_id"])["backlog"] if item else []
            if item and not any(b["id"] == item and b["status"] == "ready" for b in backlog):
                # dropped / moved by a human meanwhile
                self._records.set_order(order["id"], "dropped")
                continue
            if not self.launch(
                order, json.loads(r["variant_json"]), origin=r["origin"], channel=r["channel"]
            ):
                break
            n += 1
        return n

    def queue(self, order: WorkOrder, *, origin: str, channel: ChannelRef | None) -> None:
        self._records.queue_order(
            order, self.variant_of(order["project_id"]), origin=origin, channel=channel
        )
        self._emit("coordinator.order_queued", project_id=order["project_id"], order_id=order["id"])
        self._wake()

    def dispatch(self, choice: AgendaChoice, *, origin: str = "agenda") -> bool:
        assert choice.order is not None
        order = WorkOrder(**{**choice.order, "id": new_id("wo")})  # type: ignore[typeddict-item]
        return self.launch(order, self.variant_of(order["project_id"]), origin=origin)

    def launch(
        self, order: WorkOrder, variant: Variant, *, origin: str, purpose: str | None = None,
        improve_task: str | None = None, channel: ChannelRef | None = None, **extra: Any,
    ) -> bool:  # fmt: skip
        try:
            validate_order(order)
        except InvalidOrder as e:
            self._emit("coordinator.order_invalid", project_id=order["project_id"], error=str(e))
            self.item_status(order, "blocked", f"잘못된 작업 지시: {e}")
            return False
        if self.work_full():  # every work run shares the cap; respond keeps its slot
            self._emit("agenda.deferred", project_id=order["project_id"], kind=order["kind"],
                       reason="work cap")  # fmt: skip
            return False
        lease = self._ledger.lease(
            engine_pref=[variant["engine"] or self.engine],
            purpose=purpose or order["kind"],  # type: ignore[arg-type]
            max_minutes=order["budget"]["max_minutes"],
        )
        if lease is None:
            self._emit("agenda.deferred", project_id=order["project_id"], kind=order["kind"])
            return False
        self._records.record_dispatch(
            order, variant, origin=origin, improve_task=improve_task, channel=channel
        )
        self.item_status(order, "running", order["id"])
        self._emit(
            "agenda.dispatched",
            project_id=order["project_id"],
            order_id=order["id"],
            kind="improve" if improve_task else order["kind"],
            improve_task=improve_task,
            backlog_item_id=order["backlog_item_id"],
            origin=origin,
            **extra,
        )
        release = self.reserve_slot()
        self._tasks.spawn(self._drive(order, variant, lease, release), f"run:{order['id']}")
        return True

    async def _drive(
        self, order: WorkOrder, variant: Variant, lease: QuotaLease, release: Callable[[], None]
    ) -> None:
        try:
            res = await self._pool.run(order, variant, lease=lease)
        except asyncio.CancelledError:
            raise  # shutdown: the pool leaves the run interrupted; the item recovers on boot
        except Exception as e:  # noqa: BLE001 - e.g. UnknownProfile before the run existed
            with contextlib.suppress(Exception):
                self._ledger.release(lease)  # idempotent if the pool already released it
            self._records.set_order(order["id"], "error")
            self._emit(
                "coordinator.run_error",
                project_id=order["project_id"],
                order_id=order["id"],
                error=error_text(e, 800),
            )
            self.item_status(order, "blocked", f"실행 오류: {type(e).__name__}")
            return
        finally:
            release()
        await self.on_result(order, res)

    # --- results ---------------------------------------------------------------------

    def run_order(self, run_id: RunId) -> WorkOrder | None:
        """The order behind a run the Coordinator dispatched (None for foreign runs)."""
        try:
            order_id = self._pool.get(run_id)["order_id"]
        except KeyError:
            return None
        row = self._records.order_row(order_id)
        return order_from_json(row["order_json"]) if row else None

    async def after_run(self, run_id: RunId, res: RunResult) -> None:
        """Result of a resume / release drive: route it like the run's first result."""
        order = self.run_order(run_id)
        if order is not None:
            await self.on_result(order, res)

    async def follow(self, run_id: RunId, release: Callable[[], None]) -> None:
        """Wait for a released run and route its result. The run is work again: the caller
        reserves its slot before starting this, and `release` frees it when the run ends."""
        try:
            res = await self._pool.wait(run_id)
        finally:
            release()
        await self.after_run(run_id, res)

    async def on_result(self, order: WorkOrder, res: RunResult) -> None:
        row = self._records.order_row(order["id"])
        origin = row["origin"] if row else "agenda"
        pid, status = order["project_id"], res["status"]
        self._records.set_order(
            order["id"], "finished" if status in FINISHED else status, res["run_id"]
        )
        self._emit(
            "coordinator.run_result",
            project_id=pid,
            run_id=res["run_id"],
            order_id=order["id"],
            status=status,
            summary=res["summary"],
        )
        if pid and res["artifacts"]:
            self._projects.attach_artifacts(pid, res["artifacts"])
        held = status == "interrupted" and self.hold_item(order, res["run_id"])
        if held:
            pass  # bound to its run until the Ask closes: answer -> resume, else -> ready
        elif status == "interrupted" and not is_quota_pause(res["summary"]):
            await self._interrupted(order, res["summary"], origin)
        elif status in _ITEM_AFTER:
            self.item_status(order, _ITEM_AFTER[status], res["summary"])
        if status == "done" and order.get("backlog_item_id"):
            self._records.clear_item(order["backlog_item_id"])
        if status == "failed":
            await self._notify_failure(order, res, row, origin)
        if status == "cancelled" and res["summary"] == cancelled_summary(STUCK_REASON) and pid:
            await self._voice.say(  # the watchdog stopped it: the item is blocked, not silently
                self._voice.channel_of(pid),
                "fyi",
                f"⚠️ 멈춘 작업을 취소했어요 · {first_line(order['intent'])[:FYI_INTENT_MAX]} — "
                "진행이 없어 watchdog이 멈춤. 다시 하려면 칸반에서 ready로 옮겨 주세요.",
            )
        if status == "done":
            await self._notify_done(order, res, row, origin)
        self._wake()

    async def _notify_failure(
        self, order: WorkOrder, res: RunResult, row: sqlite3.Row | None, origin: str
    ) -> None:
        """The failure is logged for replay_failed; the project hears of it too."""
        pid = order["project_id"]
        failure = res["failure"] or {
            "what": order["intent"][:FAILURE_WHAT_MAX],
            "why": res["summary"],
        }
        self._emit(
            "coordinator.run_failed",
            project_id=pid,
            run_id=res["run_id"],
            order=json.loads(to_json(order)),
            variant=json.loads(row["variant_json"]) if row else self.variant_of(pid),
            failure=failure,
            origin=origin,
        )
        if pid and origin != "improve":  # self-improvement is never a human alert
            await self._voice.say(
                self._voice.channel_of(pid),
                "fyi",
                f"⚠️ 실패 · {first_line(order['intent'])[:FYI_INTENT_MAX]} — {failure['why']}",
            )

    async def _notify_done(
        self, order: WorkOrder, res: RunResult, row: sqlite3.Row | None, origin: str
    ) -> None:
        """A discover run's output goes to triage; a human who asked for the work hears."""
        pid = order["project_id"]
        if order["kind"] == "discover" and pid:
            await self._on_discovered(order, res)
        if origin == "human" and row is not None:
            channel = row["channel"] or self._voice.channel_of(pid)
            await self._voice.say(
                channel, "report", f"✅ 완료 · {res['summary']}", ref_id=order["id"]
            )

    async def _interrupted(self, order: WorkOrder, summary: str, origin: str) -> None:
        """A non-quota interrupt: the item waits ITEM_BACKOFF before it is dispatched again;
        the ITEM_STOP_AFTER-th in a row blocks it and tells the human once."""
        item, pid = order.get("backlog_item_id"), order["project_id"]
        if not item:
            return
        n = self._records.item_failures(item) + 1
        wait = ITEM_BACKOFF.after(n)
        if wait is not None:
            later = self._clock.now() + timedelta(seconds=wait)
            self._records.back_off_item(item, n, later)
            self._emit("coordinator.item_backoff", project_id=pid, item_id=item, failures=n,
                       not_before=later.isoformat(),
                       summary=summary[:SUMMARY_NOTE_MAX])  # fmt: skip
            self.item_status(order, "ready", f"중단 {n}회, {later:%H:%M} 이후 재시도")
            return
        self._records.clear_item(item)
        self._emit("coordinator.item_stopped", project_id=pid, item_id=item, failures=n,
                   summary=summary[:SUMMARY_NOTE_MAX])  # fmt: skip
        self.item_status(order, "blocked", f"{n}회 연속 중단: {summary}")
        if pid and origin != "improve":  # self-improvement is never a human alert
            await self._voice.say(
                self._voice.channel_of(pid),
                "fyi",
                f"⚠️ {n}번 연속 중단돼 멈췄어요 · {first_line(order['intent'])[:FYI_INTENT_MAX]} — "
                f"{summary[:FYI_SUMMARY_MAX]}. 맥미니 확인 필요",
            )

    def item_status(self, order: WorkOrder, status: str, note: str, *, force: bool = False) -> None:
        item = order.get("backlog_item_id")
        if not item or not order.get("project_id"):
            return
        cur = next(
            (b for b in self._projects.get(order["project_id"])["backlog"] if b["id"] == item),
            None,
        )
        if cur is None or cur["status"] in ("dropped", "archived"):
            return  # a human dropped it meanwhile: never resurrect
        if cur["status"] == status and not force:
            return
        note = note[:BACKLOG_NOTE_MAX]
        self._projects.update_backlog(item, status=status, note=note)  # type: ignore[arg-type]

    # --- one live run per item -------------------------------------------------------

    def _open_asks_of(self, run_id: RunId) -> list[AskView]:
        return [a for a in self._inbox.open_asks() if a["run_id"] == run_id]

    def hold_item(self, order: WorkOrder, run_id: RunId) -> bool:
        """An interrupted run that still has an open Ask keeps its item: nothing else may
        run it. The answer resumes that same run; an Ask closed without an answer
        (expired) frees the item through `release_held`. True if the item is held."""
        asks = self._open_asks_of(run_id)
        if not asks:
            return False
        self._records.set_order(order["id"], "held", run_id)
        n = self._inbox.topic_number(asks[0]["ask_id"])
        self._emit("coordinator.item_held", project_id=order["project_id"], run_id=run_id,
                   order_id=order["id"], ask_id=asks[0]["ask_id"])  # fmt: skip
        self.item_status(order, "blocked", f"#{n} 답을 기다리는 중 (같은 작업이 이어서 함)")
        return True

    def recover_items(self) -> None:
        """First tick: items left `running` by a dead process go back to ready (the daemon
        ran pool.recover_on_boot before the loop, so live runs are visible)."""
        if self._recovered:
            return
        self._recovered = True
        live = {r["order_id"] for r in self._pool.running()}
        asking: dict[str, RunId] = {}  # order id -> its (interrupted) run with an open Ask
        for a in self._inbox.open_asks():
            with contextlib.suppress(KeyError):
                asking[self._pool.get(a["run_id"])["order_id"]] = a["run_id"]
        for r in self._records.orders_in("dispatched"):
            if r["order_id"] in live:
                continue
            order = order_from_json(r["order_json"])
            if r["order_id"] in asking and self.hold_item(order, asking[r["order_id"]]):
                continue  # its Ask is still open: the answer resumes that run
            self._records.set_order(r["order_id"], "lost")
            self.item_status(order, "ready", "재시작 후 다시 대기")

    async def release_held(self) -> None:
        """Held items whose run no longer waits on an Ask: an answer is on its way to that
        run (pending resume) -> keep; the Ask expired -> the item is free again, through the
        same backoff as any other interrupt; the run finished meanwhile -> forget it."""
        resuming = self._records.resume_run_ids()
        for r in self._records.orders_in("held"):
            run_id = r["run_id"]
            if run_id in resuming or self._open_asks_of(run_id):
                continue
            try:
                run_status = self._pool.get(run_id)["status"]
            except KeyError:
                run_status = "cancelled"
            if run_status in FINISHED:
                self._records.set_order(r["order_id"], "finished")
            elif run_status == "interrupted":
                order = order_from_json(r["order_json"])
                self._records.set_order(r["order_id"], "interrupted")
                self._emit("coordinator.item_released", project_id=order["project_id"],
                           run_id=run_id, order_id=r["order_id"])  # fmt: skip
                await self._interrupted(order, "질문이 답 없이 닫혀 다시 시작", r["origin"])
            # running / waiting_human: a resume is driving it; its result routes the order
