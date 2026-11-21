"""Conversation: what humans say and what they hear back.

A tell is acked within `ack_timeout_s`; slower work finishes in the background and its
reply goes out through the Voice. The Brain classifies each message, then it becomes new
work, an Ask answer, a run control (steer / takeover / release), a goal-change
confirmation, a policy, feedback or a status line. A tell the Brain could not classify is
held and retried with backoff. Kanban human.* events (reorder, archive, comment) and lines
dropped by a run that ended first are handled here too.
"""

import asyncio
import logging
import math
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, tzinfo
from typing import Any

from swarm.aio import TaskSet
from swarm.backoff import Ladder
from swarm.clock import Clock, zone_of
from swarm.coordinator.dispatch import Dispatcher, RunControl
from swarm.coordinator.messages import (
    BACKLOG_TITLE_MAX,
    MessageClass,
    OpenAsk,
    classify,
    map_answer,
    match_option,
    resolve_ask_target,
)
from swarm.coordinator.records import CoordinatorRecords
from swarm.coordinator.resumes import Resumes
from swarm.coordinator.voice import Voice
from swarm.ids import new_id
from swarm.inbox.inbox import AskAlreadyClosed, Inbox
from swarm.ports import Brain, BrainError
from swarm.projects.store import ProjectStore, find_item
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.events import Emitter, EventLog
from swarm.text import first_line
from swarm.types import (
    BacklogItem,
    Budget,
    ChannelRef,
    Event,
    Evidence,
    Learning,
    Project,
    ProjectId,
    TellResult,
    WorkOrder,
)
from swarm.workers.contract import (
    CRITERIA_MAX,
    INTENT_MAX,
    IllegalTransition,
    LeaseExpired,
    order_from_json,
)

log = logging.getLogger(__name__)

INTERIM_ACK = "받았어요, 정리해서 답할게요"
FAILED_ACK = "⚠️ 메시지를 처리하지 못했어요. 다시 보내 주세요"
HELD_BACKOFF = Ladder((60, 300, 900))  # before each retry of a tell held after a Brain error
HUMAN_VALUE = 4  # work a human asked for is worth doing (>= threshold 3)
MIN_CLASSIFY_CONFIDENCE = 0.5  # below: guess + re-ask, change nothing
STATUS_MAX = 3_500
NO_PROJECT_ACK = "어느 프로젝트 일인지 모르겠어요. 프로젝트 토픽에 다시 보내 주세요."
_WEEKDAYS = "월화수목금토일"
ARCHIVE_REASON = "사람이 칸반에서 버림"
LIVE_RUNS = ("running", "verifying", "waiting_human")  # runs a human can talk about
ETA_STEP_S = 30 * 60
TELL_TEXT_MAX = 4_000  # the human text kept in `human.tell`
FAILED_ECHO_MAX = 300  # the human text echoed back in a failure FYI
EVENT_ERROR_MAX = 300  # a classify error in `coordinator.classify_failed`
KANBAN_LINES_MAX = 10  # dropped kanban lines shown in an FYI
STATUS_INTENT_MAX = 60  # an order's first line in a run listing


def _parse_due(raw: str | None, tz: tzinfo) -> datetime | None:
    if not raw:
        return None
    try:
        d = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return d.replace(tzinfo=tz) if d.tzinfo is None else d.astimezone(tz)


def _fmt_due(d: datetime) -> str:
    return f"{d.month}/{d.day}({_WEEKDAYS[d.weekday()]}) {d:%H:%M}"


def _ceil_half_hour(d: datetime) -> datetime:
    """Up to the next :00 / :30 in `d`'s zone (exact for offsets that are multiples of 30 min)."""
    return datetime.fromtimestamp(math.ceil(d.timestamp() / ETA_STEP_S) * ETA_STEP_S, d.tzinfo)


def _ack(ack: str, project_id: ProjectId | None = None, **kw: Any) -> TellResult:
    return TellResult(
        ack=ack,
        project_id=project_id,
        order_id=kw.get("order_id"),
        closed_ask_id=kw.get("closed_ask_id"),
    )


class Conversation:
    def __init__(
        self,
        *,
        records: CoordinatorRecords,
        dispatch: Dispatcher,
        resumes: Resumes,
        inbox: Inbox,
        pool: RunControl,
        ledger: QuotaLedger,
        projects: ProjectStore,
        brain: Brain,
        voice: Voice,
        log: EventLog,
        emit: Emitter,
        tasks: TaskSet,
        clock: Clock,
        wake: Callable[[], None],
        hold_event: Callable[[Event], None],
        ack_timeout_s: float,
    ) -> None:
        self._records = records
        self._dispatch = dispatch
        self._resumes = resumes
        self._inbox = inbox
        self._pool = pool
        self._ledger = ledger
        self._projects = projects
        self._brain = brain
        self._voice = voice
        self._log = log
        self._kv = records.kv
        self._emit = emit
        self._tasks = tasks
        self._clock = clock
        self._wake = wake
        self._hold_event = hold_event  # the facade keeps it until a respond turn
        self._ack_timeout_s = ack_timeout_s
        self._engine = dispatch.engine

    async def handled(self, event_id: str, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run `coro`; its held row is deleted when it finishes or fails — but kept when it is
        cancelled (shutdown), so the restarted daemon handles it again."""
        try:
            r = await coro
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._records.unhold(event_id)
            raise
        self._records.unhold(event_id)
        return r

    async def handle_tell(self, channel: ChannelRef, text: str) -> TellResult:
        """Ack within `ack_timeout_s`; slower work finishes in the background and its reply
        goes out through the Voice."""
        project = self._projects.by_channel(channel)
        pid = project["id"] if project else None
        self._emit("human.tell", project_id=pid, channel=channel, text=text[:TELL_TEXT_MAX])
        held = self._records.hold(  # in flight: a restart before the reply replays it (attempt 0)
            Event(id="", ts=self._clock.now(), kind="human.tell", actor="human", project_id=pid,
                  run_id=None, payload={"channel": channel, "text": text, "attempt": 0}, refs=[])
        )  # fmt: skip
        task = self._tasks.spawn(
            self.handled(held["id"], self._tell(channel, text)), f"tell:{channel}"
        )
        try:
            return await asyncio.wait_for(asyncio.shield(task), self._ack_timeout_s)
        except TimeoutError:
            task.add_done_callback(lambda t: self._late_ack(channel, t))
            return _ack(INTERIM_ACK, pid)
        except Exception:  # noqa: BLE001 - a bug never leaves the human without an answer
            log.exception("tell failed")
            return _ack(FAILED_ACK, pid)

    def _late_ack(self, channel: ChannelRef, task: asyncio.Task) -> None:
        """The human got the interim ack and was promised a reply: always send one."""
        if task.cancelled():
            return  # shutdown
        if task.exception() is not None:
            self._tasks.spawn(self._voice.reply(channel, "fyi", FAILED_ACK), "late_ack")
            return
        ack = task.result()["ack"]
        if ack != INTERIM_ACK:
            self._tasks.spawn(self._voice.reply(channel, "ack", ack), "late_ack")

    async def respond(self, ev: Event) -> None:
        """One respond turn: a held tell retried, or a kanban human.* event applied."""
        p = ev["payload"]
        if ev["kind"] == "human.tell":  # held after a Brain failure: retried with backoff
            try:
                r = await self._tell(p["channel"], p["text"], attempt=p.get("attempt", 1))
            except Exception:  # noqa: BLE001 - the human was promised a reply
                log.exception("held tell failed")
                await self._voice.reply(
                    p["channel"], "fyi", f"{FAILED_ACK}: {p['text'][:FAILED_ECHO_MAX]}"
                )
                return
            if r["ack"] != INTERIM_ACK:
                await self._voice.reply(p["channel"], "ack", r["ack"])
            return
        found = find_item(self._projects, p.get("card_key") or p.get("item_id"))
        if found is None:
            self._emit("coordinator.event_ignored", kind=ev["kind"], reason="unknown item")
            return
        project, item = found
        if ev["kind"] == "human.kanban_reorder":
            self._reorder(project, item, int(p["priority"]))
        elif ev["kind"] == "human.kanban_archive":
            self._projects.update_backlog(item["id"], status="dropped", note=ARCHIVE_REASON)
            for r in self._pool.running():
                row = self._records.order_row(r["order_id"])
                if row and order_from_json(row["order_json"])["backlog_item_id"] == item["id"]:
                    self._tasks.spawn(self._pool.cancel(r["id"], reason=ARCHIVE_REASON), "cancel")
        elif ev["kind"] == "human.kanban_comment":  # like a tell on that item's topic
            text = f"[할 일 '{item['title']}'에 단 댓글] {p.get('comment', '')}"
            r = await self._tell(project["channel"], text)
            if r["ack"] != INTERIM_ACK:
                await self._voice.reply(project["channel"], "ack", r["ack"])
        else:
            self._emit("coordinator.event_ignored", kind=ev["kind"])

    def _reorder(self, project: Project, item: BacklogItem, priority: int) -> None:
        """Kanban reorder: priority stored; moved to the top of the open items while ready
        -> it becomes the next advance, queued like work a human asked for."""
        self._projects.set_backlog_priority(item["id"], priority)
        others = [
            b["priority"]
            for b in project["backlog"]
            if b["id"] != item["id"] and b["status"] not in ("done", "dropped", "archived")
        ]
        if item["status"] != "ready" or item["id"] in self._records.reserved_items():
            return
        if others and priority <= max(others):
            return
        order = WorkOrder(
            id=new_id("wo"), project_id=project["id"], kind="advance",
            intent=(item["intent"] or item["title"])[:INTENT_MAX],
            done_criteria=list(item["done_criteria"])[:CRITERIA_MAX] or [item["title"]],
            budget=Budget(max_minutes=60), profile=project["default_profile"],
            priority=float(priority), value=item["value"], due=None,
            backlog_item_id=item["id"], action_mode="guarded",
        )  # fmt: skip
        self._dispatch.queue(order, origin="kanban", channel=project["channel"])

    async def surface_dropped(self, now: datetime) -> None:
        """Lines queued for a run that ended before it read them: the human should know."""
        since = self._kv.get_json("dropped_since")
        start = datetime.fromisoformat(since) if since else now - timedelta(days=1)
        for e in self._log.query(kinds=["run.pending_dropped"], since=start):
            if not self._records.mark_seen(e["id"]):
                continue
            lines = [f"- {x}" for x in e["payload"].get("lines", [])[:KANBAN_LINES_MAX]]
            await self._voice.say(
                self._voice.channel_of(e["project_id"]),
                "fyi",
                "\n".join(["ℹ️ FYI · 작업이 먼저 끝나 전달 못 한 지시가 있어요:", *lines]),
            )
            self._kv.set_json("dropped_since", e["ts"].isoformat())

    def _open_asks(self, channel: ChannelRef, pid: ProjectId | None) -> list[OpenAsk]:
        """Open Asks of this channel only (topic numbers are per channel)."""
        views = (
            self._inbox.open_asks(pid)
            if pid
            else [a for a in self._inbox.open_asks() if a["project_id"] is None]
        )
        return [(self._inbox.topic_number(a["ask_id"]), a) for a in views]

    async def _tell(self, channel: ChannelRef, text: str, *, attempt: int = 0) -> TellResult:
        """`attempt` = retries already made of a tell held after a Brain error."""
        project = self._projects.by_channel(channel)
        pid = project["id"] if project else None
        asks = self._open_asks(channel, pid)
        try:
            mc = await self._classify(text, pid, asks)
        except BrainError as e:
            return await self._hold_after_brain_error(channel, pid, text, attempt, e)
        return await self._route(channel, project, text, mc, asks)

    async def _classify(
        self, text: str, pid: ProjectId | None, asks: list[OpenAsk]
    ) -> MessageClass:
        runs = self._runs_of(pid, LIVE_RUNS)
        mc = await classify(
            self._brain, text, asks, now=self._clock.now(), running=[d for _, d in runs]
        )
        self._emit(
            "coordinator.classified",
            project_id=pid,
            category=mc.category,
            confidence=mc.confidence,
            summary=mc.summary,
        )
        return mc

    async def _hold_after_brain_error(
        self, channel: ChannelRef, pid: ProjectId | None, text: str, attempt: int, e: BrainError
    ) -> TellResult:
        """Held for a respond turn after HELD_BACKOFF; past its last step, ask for a resend."""
        self._emit("coordinator.classify_failed", project_id=pid, error=str(e)[:EVENT_ERROR_MAX])
        wait = HELD_BACKOFF.after(attempt + 1)
        if wait is None:  # held long enough: ask the human to resend
            await self._voice.reply(channel, "fyi", f"{FAILED_ACK}: {text[:FAILED_ECHO_MAX]}")
            return _ack(INTERIM_ACK, pid)
        later = self._clock.now() + timedelta(seconds=wait)
        payload = {"channel": channel, "text": text, "attempt": attempt + 1,
                   "not_before": later.isoformat()}  # fmt: skip
        self._hold_event(
            Event(id="", ts=self._clock.now(), kind="human.tell", actor="human",
                  project_id=pid, run_id=None, payload=payload, refs=[])
        )  # fmt: skip
        return _ack(INTERIM_ACK, pid)

    async def _route(
        self, channel: ChannelRef, project: Project | None, text: str, mc: MessageClass,
        asks: list[OpenAsk],
    ) -> TellResult:  # fmt: skip
        """One classified message to what it asks for."""
        pid = project["id"] if project else None
        if mc.category == "status":
            return _ack(await self.status(channel), pid)
        if mc.confidence < MIN_CLASSIFY_CONFIDENCE:
            return self._reask(pid, mc, asks)
        if mc.category == "ask_answer":
            return await self._answer(channel, pid, text, mc, asks)
        if mc.category in ("steer", "takeover", "release"):
            return await self._control_run(pid, text, mc)
        if project is None:
            return _ack(NO_PROJECT_ACK)
        pid = project["id"]
        if mc.category == "new_work":
            return self._new_work(channel, project, text, mc)
        if mc.category in ("goal_change", "strategy_change"):
            return await self._resumes.ask_confirm(channel, pid, text, mc)
        now = self._clock.now()
        if mc.category == "policy_add":
            self._projects.add_policy(
                pid,
                {"id": "", "text": text.strip(), "active": True, "created_at": now,
                 "source_ask": None},  # type: ignore[typeddict-item]
            )  # fmt: skip
            return _ack(f"정책 +1: {mc.summary}", pid)
        evidence = Evidence(url=None, screenshot=None, captured_at=now, text=text)
        self._projects.record_learning(
            pid, Learning(text=f"사람 피드백: {text.strip()}", evidence=[evidence])
        )
        return _ack(f"피드백 기록했어요: {mc.summary}", pid)

    def _reask(self, pid: ProjectId | None, mc: MessageClass, asks: list[OpenAsk]) -> TellResult:
        """Too unsure to act: say what was understood and change nothing."""
        self._emit("coordinator.reask", project_id=pid, reason="low confidence")
        if mc.category == "ask_answer":  # never close an Ask on a guess
            lines = "\n".join(f"#{n} {a['question']}" for n, a in asks)
            return _ack(f"어느 질문에 어떻게 답하신 건지 '#번호 답'으로 보내 주세요.\n{lines}", pid)
        return _ack(
            f"제가 이해한 건 '{mc.summary}'예요. 맞으면 조금 더 구체적으로 다시 보내 주세요.",
            pid,
        )

    def _new_work(
        self, channel: ChannelRef, project: Project, text: str, mc: MessageClass
    ) -> TellResult:
        pid = project["id"]
        title = (first_line(mc.summary) or first_line(text) or "새 일")[:BACKLOG_TITLE_MAX]
        bid = self._projects.add_backlog(
            pid,
            BacklogItem(id="", title=title, intent=text, done_criteria=[title], status="ready",
                        priority=0, value=HUMAN_VALUE),
        )  # fmt: skip
        due = _parse_due(mc.due, zone_of(self._clock))
        order = WorkOrder(
            id=new_id("wo"),
            project_id=pid,
            kind="advance",
            intent=f"{title}\n\n사람 원문: {text}"[:INTENT_MAX],
            done_criteria=[title],
            budget=Budget(max_minutes=60),
            profile=project["default_profile"],
            priority=float(HUMAN_VALUE * project["weight"]),
            value=HUMAN_VALUE,
            due=due,
            backlog_item_id=bid,
            action_mode="guarded",
        )
        self._dispatch.queue(order, origin="human", channel=channel)
        if due:
            ack = f"받았어요. {_fmt_due(due)}까지 드릴게요."
        else:  # "받았음 + 기한" even without a due: an estimate (order.due stays None)
            ack = f"받았어요. {_fmt_due(self._eta(order))}쯤까지 드릴게요: {title}"
        return _ack(ack, pid, order_id=order["id"])

    def _eta(self, order: WorkOrder) -> datetime:
        """max(now, engine reset) + the order's budget, rounded up to the half hour."""
        reset = self._ledger.next_reset(self._engine)
        now = self._clock.now()  # read after next_reset: its own now is not a cooling
        start = reset if reset > now else now
        return _ceil_half_hour(start + timedelta(minutes=order["budget"]["max_minutes"]))

    async def _answer(
        self, channel: ChannelRef, pid: ProjectId | None, text: str, mc: MessageClass,
        asks: list[OpenAsk],
    ) -> TellResult:  # fmt: skip
        target = resolve_ask_target(mc, [n for n, _ in asks])
        if target is None:
            self._emit("coordinator.reask", project_id=pid, reason="answer target")
            if not asks:
                return _ack("지금 열린 질문이 없어요.", pid)
            lines = "\n".join(f"#{n} {a['question']}" for n, a in asks)
            return _ack(f"어느 질문에 대한 답인가요? '#번호 답'으로 보내 주세요.\n{lines}", pid)
        ask = dict(asks)[target]
        choice: str | None = text.strip()
        if ask["options"]:
            strict = ask["irreversible"] or ask["kind"] in ("approval", "permission")
            choice = match_option(text, ask["options"], strict=strict)
            if choice is None and not strict:
                choice = await map_answer(self._brain, text, ask)
            if choice is None:
                self._emit("coordinator.reask", project_id=pid, reason="answer option")
                opts = " / ".join(ask["options"])
                return _ack(
                    f"#{target}에 어떻게 답할지 못 정했어요. 다음 중 하나로 답해 주세요: {opts}",
                    pid,
                )
        try:
            await self._inbox.answer(ask["ask_id"], choice=choice, raw_text=text, via=channel)
        except AskAlreadyClosed:
            return _ack(f"#{target}은 이미 닫힌 질문이에요.", pid)
        self._wake()  # resume at the next tick
        return _ack(f"{choice}로 진행합니다.", pid, closed_ask_id=ask["ask_id"])

    async def _control_run(self, pid: ProjectId | None, text: str, mc: MessageClass) -> TellResult:
        statuses = ("waiting_human",) if mc.category == "release" else LIVE_RUNS
        runs = self._runs_of(pid, statuses)
        hint = mc.target_run_hint
        picked = [r for r in runs if hint and hint in r[1]] if hint else []
        if len(picked) != 1:
            picked = runs if len(runs) == 1 else []
        if not picked:
            if not runs:
                return _ack("지금 해당하는 작업이 없어요.", pid)
            listing = "\n".join(d for _, d in runs)
            return _ack(f"어느 작업인가요? 작업 ID를 붙여 다시 보내 주세요.\n{listing}", pid)
        run_id = picked[0][0]
        try:
            if mc.category == "steer":
                mode = await self._pool.steer(run_id, text=text)
                return _ack(
                    "바로 전달했어요." if mode == "live" else "다음 단계부터 반영할게요.", pid
                )
            if mc.category == "takeover":
                info = await self._pool.takeover(run_id)
                extra = f"\n{info['instructions']}" if info.get("instructions") else ""
                return _ack(
                    f"넘겨드려요: {info['profile']} 프로필 브라우저에서 처리하고 "
                    f"끝나면 '됐어'라고 보내 주세요.{extra}",
                    pid,
                )
            if self._dispatch.work_full():  # the released run is work again: it needs a work slot
                raise LeaseExpired("work cap")
            await self._pool.release(run_id, note=text)
        except LeaseExpired:
            return _ack("지금 엔진이 꽉 차 있어요. 잠시 후 다시 '됐어'라고 보내 주세요.", pid)
        except (IllegalTransition, KeyError, RuntimeError) as e:
            return _ack(f"그 작업에는 지금 할 수 없어요: {e}", pid)
        release = self._dispatch.reserve_slot()  # counted against the work cap until it ends
        self._tasks.spawn(self._dispatch.follow(run_id, release), f"release:{run_id}")
        return _ack("이어서 진행할게요.", pid)

    # --- status ---------------------------------------------------------------------------

    async def status(self, channel: ChannelRef) -> str:
        """Goal, KRs, backlog counts and live runs: of the channel's project, else of all."""
        project = self._projects.by_channel(channel)
        lines: list[str] = []
        for p in [project] if project else self._projects.list():
            counts: dict[str, int] = {}
            for b in p["backlog"]:
                counts[b["status"]] = counts.get(b["status"], 0) + 1
            todo = " · ".join(f"{k} {v}" for k, v in counts.items()) or "없음"
            runs = self._runs_of(p["id"], LIVE_RUNS)
            lines.append(f"📋 {p['name']} · {p['goal']['objective']}")
            lines += [
                f"- {k['metric']} {k['current']:g}/{k['target']:g}"
                for k in p["goal"]["key_results"]
            ]
            lines.append(f"할 일: {todo} · 실행 중 {len(runs)}")
            if project:
                lines += [f"- {d}" for _, d in runs]
                lines += [f"- #{n} {a['question']}" for n, a in self._open_asks(channel, p["id"])]
        if not lines:
            lines.append("프로젝트가 아직 없어요.")
        reset = self._ledger.next_reset()
        if reset > self._clock.now():  # read after next_reset: its own now is not a cooling
            lines.append(f"엔진 쉬는 중 ~{reset:%H:%M}")
        if self._ledger.disabled(self._engine):  # logged out
            lines.append("⚠️ claude 로그인 필요 (맥미니에서 claude 로그인)")
        return "\n".join(lines)[:STATUS_MAX]

    def _runs_of(self, pid: ProjectId | None, statuses: tuple[str, ...]) -> list[tuple[str, str]]:
        """(run id, one-line description) of the project's runs in `statuses` (all if no pid)."""
        out = []
        for r in self._pool.running():
            if r["status"] not in statuses or (pid and self._pool.run_project(r["id"]) != pid):
                continue
            row = self._records.order_row(r["order_id"])
            intent = (
                first_line(order_from_json(row["order_json"])["intent"])[:STATUS_INTENT_MAX]
                if row
                else ""
            )
            out.append((r["id"], f"{r['id']} ({r['status']}): {intent}"))
        return out
