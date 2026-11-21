"""Reports: numbers are aggregated by code, prose is the LLM's.

Format: 결론 -> 바뀐 것 -> 한 것 -> 목표까지 (KR 간격, 건강 상태) -> 나한테 필요한 것.
Every number in the rendered text comes from `ReportFacts`. LLM prose that quotes a number
not in the facts is dropped (the deterministic line still carries the real numbers). If the
Brain fails, a numbers-only template is sent instead. All projects 순항 -> <= 5 lines.

ReportScheduler sends the 08:00 daily briefing to General and the Friday 17:00 weekly per
project. A send date is stored only after a successful send, so a failed report is retried
after REPORT_BACKOFF and never sent twice on one day.
"""

import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Literal

from pydantic import BaseModel

from swarm.aio import TaskSet
from swarm.backoff import Ladder
from swarm.clock import Clock, zone_of
from swarm.coordinator.voice import Voice
from swarm.inbox.inbox import Inbox
from swarm.ports import Brain, BrainError
from swarm.projects.store import ProjectStore
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.events import Emitter, EventLog
from swarm.store.kv import KvTable
from swarm.types import ChannelRef, Project, ProjectId

ReportKind = Literal["daily", "weekly"]
Health = Literal["순항", "위험", "이탈"]

DAILY_WINDOW = timedelta(hours=24)
WEEKLY_WINDOW = timedelta(days=7)
REPORT_TIMEOUT_S = 60
# ledger.next_reset() answers with its own now (a moment after the report's `now`) when no
# engine cools; a real cooling period is minutes to hours, so 5 s of slack tells them apart
CLOCK_SLACK = timedelta(seconds=5)
REPORT_BACKOFF = Ladder((300, 900, 3600))  # after a failed report: 5m, 15m, then hourly
_TITLE = {"daily": "일일 보고", "weekly": "주간 보고"}
_NUMBER = re.compile(r"\d+(?:\.\d+)?")


@dataclass(frozen=True, kw_only=True)
class KrFact:
    metric: str
    current: float
    target: float


@dataclass(frozen=True, kw_only=True)
class AskLine:
    number: int
    question: str
    project_id: ProjectId | None


@dataclass(frozen=True, kw_only=True)
class ProjectFacts:
    id: ProjectId
    name: str
    runs_done: int
    runs_failed: int
    artifacts: int
    learnings: int
    krs: tuple[KrFact, ...]
    open_asks: tuple[AskLine, ...]


@dataclass(frozen=True, kw_only=True)
class ReportFacts:
    kind: ReportKind
    since: datetime
    until: datetime
    runs_done: int
    runs_failed: int
    artifacts: int
    learnings: int
    quota_hits: int  # engine-wide, even in a per-project report
    cooling_until: datetime | None
    projects: tuple[ProjectFacts, ...]
    open_asks: tuple[AskLine, ...]


class ProjectNarration(BaseModel):
    project_id: str
    health: Health
    note: str


class ReportNarration(BaseModel):
    """Prose only: no numeric fields, so the LLM never owns a number."""

    conclusion: str
    changed: list[str]  # 바뀐 것 (세상)
    done: list[str]  # 한 것
    projects: list[ProjectNarration]


# --- aggregation (code) ---------------------------------------------------------------


@dataclass(frozen=True)
class _RunFacts:
    """What the event log says happened in the window (events after `now` are ignored)."""

    last_status: dict[str, tuple[ProjectId | None, str]]  # run id -> (project, final status)
    artifacts: dict[ProjectId | None, int]
    learnings: dict[ProjectId | None, int]

    def runs(self, status: str, pid: ProjectId | None = None, every: bool = False) -> int:
        return sum(1 for p, s in self.last_status.values() if s == status and (every or p == pid))


def _run_facts(
    log: EventLog, since: datetime, now: datetime, project_id: ProjectId | None
) -> _RunFacts:
    last_status: dict[str, tuple[ProjectId | None, str]] = {}
    artifacts: dict[ProjectId | None, int] = {}
    learnings: dict[ProjectId | None, int] = {}
    kinds = (
        "run.finished",
        "project.artifacts_attached",
        "project.learning_recorded",
    )
    for ev in log.query(project_id=project_id, kinds=kinds, since=since):
        if ev["ts"] > now:
            continue
        pid = ev["project_id"]
        if ev["kind"] == "run.finished" and ev["run_id"]:
            last_status[ev["run_id"]] = (pid, ev["payload"].get("status", ""))
        elif ev["kind"] == "project.artifacts_attached":
            artifacts[pid] = artifacts.get(pid, 0) + len(ev["payload"].get("artifact_ids", []))
        elif ev["kind"] == "project.learning_recorded":
            learnings[pid] = learnings.get(pid, 0) + 1
    return _RunFacts(last_status, artifacts, learnings)


def _ask_facts(inbox: Inbox, project_id: ProjectId | None) -> tuple[AskLine, ...]:
    return tuple(
        AskLine(
            number=inbox.topic_number(a["ask_id"]),
            question=a["question"],
            project_id=a["project_id"],
        )
        for a in inbox.open_asks(project_id)
    )


def _quota_facts(
    log: EventLog, ledger: QuotaLedger, since: datetime, now: datetime
) -> tuple[int, datetime | None]:
    """(quota hits in the window, when the cooldown ends). Quota is engine-wide (events carry
    no project), so it is never filtered by project."""
    hits = sum(
        1 for ev in log.query(kinds=("engine.quota_exhausted",), since=since) if ev["ts"] <= now
    )
    reset = ledger.next_reset()  # = the ledger's own (later) now when nothing cools
    return hits, reset if reset > now + CLOCK_SLACK else None


def _project_facts(p: Project, facts: _RunFacts, asks: tuple[AskLine, ...]) -> ProjectFacts:
    return ProjectFacts(
        id=p["id"],
        name=p["name"],
        runs_done=facts.runs("done", p["id"]),
        runs_failed=facts.runs("failed", p["id"]),
        artifacts=facts.artifacts.get(p["id"], 0),
        learnings=facts.learnings.get(p["id"], 0),
        krs=tuple(
            KrFact(metric=k["metric"], current=k["current"], target=k["target"])
            for k in p["goal"]["key_results"]
        ),
        open_asks=tuple(a for a in asks if a.project_id == p["id"]),
    )


def _build(
    kind: ReportKind,
    window: timedelta,
    log: EventLog,
    projects: ProjectStore,
    inbox: Inbox,
    ledger: QuotaLedger,
    now: datetime,
    project_id: ProjectId | None,
) -> ReportFacts:
    since = now - window
    facts = _run_facts(log, since, now, project_id)
    quota_hits, cooling_until = _quota_facts(log, ledger, since, now)
    asks = _ask_facts(inbox, project_id)
    pfacts = tuple(
        _project_facts(p, facts, asks)
        for p in projects.list()
        if project_id is None or p["id"] == project_id
    )
    return ReportFacts(
        kind=kind,
        since=since,
        until=now,
        runs_done=facts.runs("done", every=True),
        runs_failed=facts.runs("failed", every=True),
        artifacts=sum(facts.artifacts.values()),
        learnings=sum(facts.learnings.values()),
        quota_hits=quota_hits,
        cooling_until=cooling_until,
        projects=pfacts,
        open_asks=asks,
    )


def build_daily(
    log: EventLog,
    projects: ProjectStore,
    inbox: Inbox,
    ledger: QuotaLedger,
    now: datetime,
    project_id: ProjectId | None = None,
) -> ReportFacts:
    return _build("daily", DAILY_WINDOW, log, projects, inbox, ledger, now, project_id)


def build_weekly(
    log: EventLog,
    projects: ProjectStore,
    inbox: Inbox,
    ledger: QuotaLedger,
    now: datetime,
    project_id: ProjectId | None = None,
) -> ReportFacts:
    return _build("weekly", WEEKLY_WINDOW, log, projects, inbox, ledger, now, project_id)


# --- rendering --------------------------------------------------------------------------


def _num(x: float) -> str:
    return f"{x:g}"


def _totals(f: ReportFacts) -> str:
    s = f"완료 {f.runs_done} · 실패 {f.runs_failed} · 산출물 {f.artifacts} · 배움 {f.learnings}"
    if f.quota_hits:
        s += f" · 엔진 한도(전체) {f.quota_hits}회"
    if f.cooling_until is not None:
        s += f" (쉼: {f.cooling_until.strftime('%m-%d %H:%M')}까지)"
    return s


def _kr(k: KrFact) -> str:
    left = k.target - k.current
    tail = "달성" if left <= 0 else f"남은 {_num(left)}"
    return f"{k.metric} {_num(k.current)}/{_num(k.target)} ({tail})"


def _ask(a: AskLine, f: ReportFacts) -> str:
    if len(f.projects) > 1:
        name = next((p.name for p in f.projects if p.id == a.project_id), "General")
        return f"#{a.number} [{name}] {a.question}"
    return f"#{a.number} {a.question}"


def _facts_conclusion(f: ReportFacts) -> str:
    return f"완료 {f.runs_done} · 실패 {f.runs_failed} · 열린 질문 {len(f.open_asks)}"


def _allowed_numbers(f: ReportFacts) -> set[str]:
    nums: list[float] = [
        f.runs_done,
        f.runs_failed,
        f.artifacts,
        f.learnings,
        f.quota_hits,
        len(f.open_asks),
        len(f.projects),
    ]
    for p in f.projects:
        nums += [p.runs_done, p.runs_failed, p.artifacts, p.learnings, len(p.open_asks)]
        for k in p.krs:
            nums += [k.current, k.target, max(k.target - k.current, 0)]
    nums += [a.number for a in f.open_asks]
    # No date/time parts of since/until/cooling_until: the narration has no use for them (the
    # rendered time lines are templated), and allowing them would whitelist 0~59 wholesale.
    return {_num(float(n)) for n in nums}


def _clean(text: str, allowed: set[str]) -> str | None:
    """Drop LLM prose that quotes a number the facts do not contain.

    Limitation: the guard is value-based, not context-based. A fact value quoted in the wrong
    place ("실패 2" when 2 is the done count) passes, and legitimate outside numbers ("3월
    출시") are dropped. The templated lines always carry the real numbers either way.
    """
    text = " ".join(text.split())
    if not text:
        return None
    for tok in _NUMBER.findall(text):
        if _num(float(tok)) not in allowed:
            return None
    return text


def _clean_all(items: Iterable[str], allowed: set[str]) -> list[str]:
    return [c for c in (_clean(t, allowed) for t in items) if c]


def _facts_prompt(f: ReportFacts) -> str:
    lines = [
        f"{_TITLE[f.kind]}를 쓴다. 기간: {f.since.isoformat(timespec='minutes')} ~ "
        f"{f.until.isoformat(timespec='minutes')}.",
        "아래 사실만 근거로 쓴다. 숫자는 사실에 있는 값만 인용하고, 새 숫자를 만들지 않는다.",
        "conclusion: 한 줄 결론. changed: 세상에서 바뀐 것. done: 우리가 한 것.",
        "projects: 프로젝트별 건강 상태(순항/위험/이탈)와 한 줄 메모.",
        "",
        f"전체: {_totals(f)} · 열린 질문 {len(f.open_asks)}",
    ]
    for p in f.projects:
        lines.append(
            f"[{p.id}] {p.name}: 완료 {p.runs_done} · 실패 {p.runs_failed} · "
            f"산출물 {p.artifacts} · 배움 {p.learnings} · 열린 질문 {len(p.open_asks)}"
        )
        lines += [f"  KR {_kr(k)}" for k in p.krs]
    lines += [f"질문 {_ask(a, f)}" for a in f.open_asks]
    return "\n".join(lines)


def _needs_lines(f: ReportFacts) -> list[str]:
    return [f"- {_ask(a, f)}" for a in f.open_asks] or ["- 없음"]


def render_template(f: ReportFacts) -> str:
    """Numbers-only report (LLM failed)."""
    lines = [f"📊 {_TITLE[f.kind]} · {_facts_conclusion(f)}", "한 것", f"- {_totals(f)}"]
    lines.append("목표까지")
    for p in f.projects:
        lines.append(f"- {p.name}")
        lines += [f"  - {_kr(k)}" for k in p.krs]
    lines.append("나한테 필요한 것")
    lines += _needs_lines(f)
    return "\n".join(lines)


def _render_compact(f: ReportFacts, n: ReportNarration, conclusion: str, allowed: set[str]) -> str:
    """All 순항: 결론 -> 바뀐 것 -> 한 것 -> 목표까지 -> 필요한 것, one line each (<= 5)."""
    goals = "; ".join(
        f"{p.name} 순항" + (f" ({', '.join(_kr(k) for k in p.krs)})" if p.krs else "")
        for p in f.projects
    )
    needs = "; ".join(_ask(a, f) for a in f.open_asks) or "없음"
    changed = "; ".join(_clean_all(n.changed, allowed)) or "없음"
    lines = [
        f"📊 {_TITLE[f.kind]} · {conclusion}",
        f"바뀐 것: {changed}",
        f"한 것: {_totals(f)}",
    ]
    if goals:
        lines.append(f"목표까지: {goals}")
    lines.append(f"나한테 필요한 것: {needs}")
    return "\n".join(lines)


def _render_full(f: ReportFacts, n: ReportNarration, conclusion: str, allowed: set[str]) -> str:
    by_id = {p.project_id: p for p in n.projects}
    lines = [f"📊 {_TITLE[f.kind]} · {conclusion}", "바뀐 것"]
    lines += [f"- {c}" for c in _clean_all(n.changed, allowed)] or ["- 없음"]
    lines += ["한 것", f"- {_totals(f)}"]
    lines += [f"- {d}" for d in _clean_all(n.done, allowed)]
    lines.append("목표까지")
    for p in f.projects:
        pn = by_id.get(p.id)
        head = f"- {p.name}"
        if pn is not None:
            head += f" [{pn.health}]"
            note = _clean(pn.note, allowed)
            if note:
                head += f" {note}"
        lines.append(head)
        lines += [f"  - {_kr(k)}" for k in p.krs]
    lines.append("나한테 필요한 것")
    lines += _needs_lines(f)
    return "\n".join(lines)


async def render_report(facts: ReportFacts, brain: Brain) -> str:
    try:
        n = await brain.decide(
            purpose="report",
            prompt=_facts_prompt(facts),
            schema=ReportNarration,
            timeout_s=REPORT_TIMEOUT_S,
        )
    except BrainError:
        return render_template(facts)
    allowed = _allowed_numbers(facts)
    conclusion = _clean(n.conclusion, allowed) or _facts_conclusion(facts)
    health = {p.project_id: p.health for p in n.projects}
    if all(health.get(p.id) == "순항" for p in facts.projects):
        return _render_compact(facts, n, conclusion, allowed)
    return _render_full(facts, n, conclusion, allowed)


class ReportScheduler:
    def __init__(
        self,
        *,
        kv: KvTable,
        projects: ProjectStore,
        voice: Voice,
        emit: Emitter,
        tasks: TaskSet,
        clock: Clock,
        general: ChannelRef,
        daily_at: time,
        weekly_at: time,
        render: Callable[[ReportKind, ProjectId | None], Awaitable[str]],
    ) -> None:
        self._kv = kv
        self._projects = projects
        self._voice = voice
        self._emit = emit
        self._tasks = tasks
        self._clock = clock
        self._general = general
        self._daily_at = daily_at
        self._weekly_at = weekly_at
        self._render = render
        self._reporting: set[str] = set()  # report kv keys being rendered / sent
        self._report_retry: dict[str, tuple[int, datetime]] = {}  # key -> (failures, next try)

    def maybe_send(self, now: datetime) -> None:
        """08:00 daily briefing to General, Fri 17:00 weekly per project. The send date is
        stored only after a successful render + send; while one is in flight it is not
        started again, and a failed one is retried at the next tick."""
        local = now.astimezone(zone_of(self._clock))
        today = local.date().isoformat()
        due: list[tuple[str, ChannelRef, ReportKind, ProjectId | None]] = []
        if local.time() >= self._daily_at:
            due.append(("daily_sent", self._general, "daily", None))
        if local.weekday() == 4 and local.time() >= self._weekly_at:
            due += [(f"weekly_sent:{p['id']}", p["channel"], "weekly", p["id"])
                    for p in self._projects.list()]  # fmt: skip
        for key, channel, kind, pid in due:
            if self._kv.get_json(key) == today or key in self._reporting:
                continue
            _, next_try = self._report_retry.get(key, (0, now))
            if now < next_try:
                continue  # a failing send never re-renders faster than the backoff
            self._reporting.add(key)
            ref = f"report:{kind}:{pid or 'all'}:{today}"
            self._tasks.spawn(self._send(key, today, channel, kind, pid, ref), ref)

    async def _send(
        self, key: str, today: str, channel: ChannelRef, kind: ReportKind,
        pid: ProjectId | None, ref: str,
    ) -> None:  # fmt: skip
        try:
            text = await self._render(kind, pid)
            if await self._voice.say(channel, "report", text, ref_id=ref):
                self._kv.set_json(key, today)
                self._report_retry.pop(key, None)
                return
            error = "send failed"
        except Exception as e:  # noqa: BLE001 - retried after the backoff
            error = repr(e)
        finally:
            self._reporting.discard(key)
        failures = self._report_retry.get(key, (0, self._clock.now()))[0] + 1
        wait = REPORT_BACKOFF.capped(failures)
        self._report_retry[key] = (failures, self._clock.now() + timedelta(seconds=wait))
        self._emit(
            "coordinator.report_failed", project_id=pid, kind=kind, error=error, retry_in_s=wait
        )
