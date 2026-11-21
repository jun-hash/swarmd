"""Resumes: closed Asks back to their runs, and goal-change confirmations.

Outcomes the Inbox closed are drained into a durable resume list in the same transaction
(exactly once), then each one resumes its run when a work slot and a lease allow. A failed
resume backs off and finally gives up, telling the human. The Coordinator's own Asks
(goal or strategy changes it must confirm) are applied from that same list.
"""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import datetime, timedelta

from swarm.aio import TaskSet
from swarm.backoff import Ladder
from swarm.clock import Clock
from swarm.coordinator.dispatch import Dispatcher, RunControl
from swarm.coordinator.messages import MessageClass
from swarm.coordinator.records import CoordinatorRecords
from swarm.coordinator.voice import Voice
from swarm.ids import new_id
from swarm.inbox.inbox import Inbox
from swarm.projects.store import ProjectStore
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.db import Database
from swarm.store.events import Emitter
from swarm.text import error_text
from swarm.types import AskDraft, AskOutcome, ChannelRef, ProjectId, QuotaLease, RunId, TellResult

log = logging.getLogger(__name__)

RESUME_BACKOFF = Ladder((60, 300, 900, 3600))  # before attempt 2, 3, 4, 5; then give up
CONFIRM_OPTIONS = ["바꿔요", "그대로"]


class Resumes:
    def __init__(
        self,
        *,
        records: CoordinatorRecords,
        dispatch: Dispatcher,
        inbox: Inbox,
        pool: RunControl,
        ledger: QuotaLedger,
        projects: ProjectStore,
        voice: Voice,
        emit: Emitter,
        tasks: TaskSet,
        clock: Clock,
        db: Database,
    ) -> None:
        self._records = records
        self._dispatch = dispatch
        self._inbox = inbox
        self._pool = pool
        self._ledger = ledger
        self._projects = projects
        self._voice = voice
        self._emit = emit
        self._tasks = tasks
        self._clock = clock
        self._db = db
        self._engine = dispatch.engine
        self._deferred: dict[str, str] = {}  # ask id -> why its resume last waited

    def _tick_error(self, phase: str, e: Exception) -> None:
        log.error("agenda tick phase %s failed", phase, exc_info=e)
        with contextlib.suppress(Exception):
            self._emit("agenda.tick_error", phase=phase, error=error_text(e, 800))

    # --- closed Asks -> resume -------------------------------------------------------

    async def inbox_tick(self, now: datetime) -> None:
        """Asks past their deadline closed with the default: the human hears which."""
        for o in await self._inbox.tick(now):  # informational: drain_closed delivers them
            pid = self.ask_project(o["run_id"]) or self._pool.run_project(o["run_id"])
            n = self._inbox.topic_number(o["ask_id"])
            await self._voice.say(
                self._voice.channel_of(pid),
                "fyi",
                f"ℹ️ FYI · #{n} 답이 없어 기본값으로 진행: {o['choice']}",
            )

    async def drain_closed(self) -> None:
        """Closed Asks into the durable resume list (exactly once), then apply the due
        confirmations and audit expired ones; worker outcomes wait for `start`."""
        with self._db.transaction():  # exactly once: drained only together with the insert
            self._records.add_resumes(self._inbox.drain_closed())
        # route from the table, not from `outs`: rows left by a failed phase or a restart
        now = self._clock.now()
        for r in self._records.pending_resumes():
            o = r.outcome
            if r.next_attempt_at and r.next_attempt_at > now:
                continue
            try:
                if o["status"] == "expired":  # its run already ended: audit, never resume
                    self._emit("coordinator.ask_expired", run_id=o["run_id"], ask_id=o["ask_id"])
                    self._records.drop_resume(o["ask_id"])
                elif self.ask_project(o["run_id"]):  # our own confirmation Ask
                    await self.apply_confirmation(o)
                    self._records.drop_resume(o["ask_id"])
            except Exception as e:  # noqa: BLE001 - one bad row never blocks the others
                self._tick_error("drain", e)
                await self._confirm_failed(o, r.attempts + 1, now)

    async def _confirm_failed(self, o: AskOutcome, attempts: int, now: datetime) -> None:
        """A confirmation that could not be applied: back off like a resume, then give up."""
        wait = RESUME_BACKOFF.after(attempts)
        if wait is not None:
            self._records.retry_resume(o["ask_id"], attempts, now + timedelta(seconds=wait))
            return
        self._records.drop_resume(o["ask_id"])
        pid = self.ask_project(o["run_id"])
        self._emit("coordinator.confirm_failed", project_id=pid, ask_id=o["ask_id"])
        await self._voice.say(
            self._voice.channel_of(pid),
            "fyi",
            "⚠️ 확인한 변경을 적용하지 못했어요, 다시 알려 주세요.",
        )

    def due(self, now: datetime) -> list[AskOutcome]:
        """Worker outcomes due for a (re)try; confirmations and expired rows go via drain."""
        out = []
        for r in self._records.pending_resumes():
            o = r.outcome
            due = r.next_attempt_at is None or r.next_attempt_at <= now
            if due and o["status"] != "expired" and not self.ask_project(o["run_id"]):
                out.append(o)
        return out

    def start(self, o: AskOutcome) -> None:
        """Resume the outcome's run when a work slot and a lease allow; else wait (logged)."""
        if o["status"] == "expired" or self.ask_project(o["run_id"]):
            return  # never pool.resume an expired outcome or one of our own Asks
        if self._dispatch.work_full():  # a resume is work: it waits for a work slot
            self._resume_deferred(o, reason="work cap")
            return
        engine = self._engine
        with contextlib.suppress(KeyError):
            engine = self._pool.get(o["run_id"])["variant"]["engine"] or self._engine
        order = self._dispatch.run_order(o["run_id"])
        minutes = order["budget"]["max_minutes"] if order else 60
        lease = self._ledger.lease(engine_pref=[engine], purpose="respond", max_minutes=minutes)
        if lease is None:  # the answer is kept; only the resume waits
            self._resume_deferred(o, reason="no lease", until=self._ledger.next_reset().isoformat())
            return
        self._deferred.pop(o["ask_id"], None)
        self._records.mark_resume_inflight(o["ask_id"])
        release = self._dispatch.reserve_slot()  # counted against the work cap until it ends
        self._tasks.spawn(self._resume(o, lease, release), f"resume:{o['ask_id']}")

    def _resume_deferred(self, o: AskOutcome, *, reason: str, until: str | None = None) -> None:
        """Logged once per (ask, reason, until), not on every tick of a 5 h cooling."""
        key = f"{reason}:{until}"
        if self._deferred.get(o["ask_id"]) == key:
            return
        self._deferred[o["ask_id"]] = key
        self._emit("coordinator.resume_deferred", run_id=o["run_id"], ask_id=o["ask_id"],
                   reason=reason, until=until)  # fmt: skip

    async def _resume(self, o: AskOutcome, lease: QuotaLease, release: Callable[[], None]) -> None:
        try:
            try:
                res = await self._pool.resume(o["run_id"], outcome=o, lease=lease)
            finally:
                release()
        except asyncio.CancelledError:
            raise  # left inflight -> pending again at the next start
        except Exception as e:  # noqa: BLE001 - the pool returned the unused lease
            attempts = self._records.resume_attempts(o["ask_id"]) + 1
            wait = RESUME_BACKOFF.after(attempts)
            give_up = isinstance(e, KeyError) or wait is None
            if give_up:
                self._records.drop_resume(o["ask_id"])
            else:
                later = self._clock.now() + timedelta(seconds=wait)
                self._records.retry_resume(o["ask_id"], attempts, later)
            self._emit(
                "coordinator.resume_failed" if give_up else "coordinator.resume_retry",
                run_id=o["run_id"],
                ask_id=o["ask_id"],
                attempts=attempts,
                error=error_text(e, 500),
            )
            if give_up:
                await self._resume_gave_up(o, e)
            return
        self._records.drop_resume(o["ask_id"])
        self._emit(
            "coordinator.resumed", run_id=o["run_id"], ask_id=o["ask_id"], status=res["status"]
        )
        await self._dispatch.after_run(o["run_id"], res)

    async def _resume_gave_up(self, o: AskOutcome, e: Exception) -> None:
        """The human was told "X로 진행합니다": say it did not reach the work after all."""
        order = self._dispatch.run_order(o["run_id"])
        pid = order["project_id"] if order else self._pool.run_project(o["run_id"])
        n = "?"
        with contextlib.suppress(Exception):
            n = str(self._inbox.topic_number(o["ask_id"]))
        why = error_text(e, 200)
        await self._voice.say(
            self._voice.channel_of(pid),
            "fyi",
            f"⚠️ #{n} 답({o['choice']})을 작업에 전달하지 못했어요 ({why}). "
            "필요하면 새로 시켜 주세요.",
        )
        if order:
            self._records.set_order(order["id"], "error")  # no longer held for that run
            self._dispatch.item_status(order, "blocked", f"답 전달 실패 #{n}: {why}", force=True)

    # --- coordinator-owned Asks ----------------------------------------------------------

    def ask_project(self, run_id: RunId) -> ProjectId | None:
        """Project of a Coordinator-raised Ask (goal-change confirmation), else None."""
        return self._records.confirm_project(run_id)

    async def apply_confirmation(self, o: AskOutcome) -> None:
        """The human answered a goal or strategy change: a strategy becomes a policy; a goal
        stays the human's to edit in GOAL.md."""
        r = self._records.confirm(o["run_id"])
        pid, summary = r["project_id"], r["summary"]
        channel = self._voice.channel_of(pid)
        if o["choice"] != CONFIRM_OPTIONS[0]:
            self._emit("coordinator.change_declined", project_id=pid, summary=summary)
            await self._voice.say(channel, "fyi", f"ℹ️ FYI · 그대로 둘게요: {summary}")
            return
        self._emit(
            "coordinator.change_confirmed", project_id=pid, category=r["category"], summary=summary
        )
        if r["category"] == "strategy_change":  # policies reach every worker of the project
            self._projects.add_policy(
                pid,
                {"id": "", "text": f"전략: {summary}", "active": True,
                 "created_at": self._clock.now(), "source_ask": o["ask_id"]},  # type: ignore[typeddict-item]
            )  # fmt: skip
            await self._voice.say(channel, "fyi", f"✅ 전략을 바꿨어요 (정책 +1): {summary}")
            return
        slug = self._projects.get(pid)["slug"]
        await self._voice.say(
            channel,
            "fyi",
            f"✅ 확인했어요: {summary}\n목표·KR은 사람 소유라 GOAL.md에서 고쳐 주세요 "
            f"(~/swarm/projects/{slug}/GOAL.md, 저장하면 다음 tick에 반영).",
        )

    async def ask_confirm(
        self, channel: ChannelRef, pid: ProjectId, text: str, mc: MessageClass
    ) -> TellResult:
        """Goals belong to the human: never applied from a message, confirmed by an Ask."""
        run_id = new_id("coord")  # pseudo run: the Ask's owner is the Coordinator
        self._records.add_confirm(run_id, pid, mc.category, mc.summary, text)
        what = "목표를" if mc.category == "goal_change" else "전략을"
        ticket = await self._inbox.raise_ask(
            run_id,
            AskDraft(kind="decision", question=f"{what} 이렇게 바꿀까요? {mc.summary}",
                     options=list(CONFIRM_OPTIONS), recommended=None, default_after=None,
                     irreversible=True, evidence=[]),  # irreversible: no policy, no default
        )  # fmt: skip
        n = self._inbox.topic_number(ticket["ask_id"])
        await self._voice.say(
            channel,
            "ask",
            f"🙋 결정 · #{n} {what} 이렇게 바꿀까요? {mc.summary}\n"
            "'바꿔요' 또는 '그대로'로 답해 주세요.",
            ref_id=ticket["ask_id"],
        )
        return TellResult(
            ack=f"바로 바꾸지 않고 확인부터 할게요 (#{n}).",
            project_id=pid,
            order_id=None,
            closed_ask_id=None,
        )
