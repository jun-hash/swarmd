"""Pure agenda decision: top-down, first match.

`choose_next` reads no clock and does no IO: everything it needs is in `AgendaState` and
`now`. The orders it builds carry `id=""`; the caller assigns an id when it dispatches them.

Scope notes:
- World-change events (page diff, RSS, replies) are not implemented (`Project.watches` is
  empty), so the "세상 변화 이벤트" row has no input here.
- Defaulted Asks are closed by `Inbox.tick`; they reach `closed_asks` through `drain_closed`
  together with answered ones, so both rows ("Ask 답" and "기한 지남") become `resume`.
  `Inbox.tick` only defaults reversible Asks; the coordinator must never apply a
  recommended option to an irreversible Ask. `expired` outcomes are never resumed (their run
  already ended): they are recorded in `skipped` and the table continues.
- Row order deviation from the table: sleep (no engine) and the parallel cap are checked
  right after respond/resume and BEFORE advance/discover/improve, because discover needs an
  engine too, not only advance and improve as the table words it. respond/resume stay above
  them ("할당량 부족해도 우선 배정").
- Quota is not budgeted: `engine_available` only says the engine is not cooling
  or auth-disabled; the parallel cap is checked against `running_count`.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

from swarm.types import (
    AskOutcome,
    BacklogItem,
    Budget,
    Event,
    Goal,
    OrderKind,
    ProjectId,
    WorkOrder,
)
from swarm.workers.contract import CRITERIA_MAX, INTENT_MAX

VALUE_THRESHOLD = 3  # value(1~5) >= 3 is worth doing
PARALLEL_CAP = 3  # claude concurrent cap
STARVING_AFTER = timedelta(hours=48)  # no advance for 48h -> priority at the next chance
DISCOVER_EVERY = timedelta(hours=24)
EST_COST = 1  # there is no per-item cost estimate: value x weight / 1
DEFAULT_MAX_MINUTES = 60
SKIPPED_MAX = 20  # agenda.tick payload stays far below the 16KB event limit
IMPROVE_ROTATION = ["replay_failed", "memory_consolidate"]  # eval and mining are not implemented

ChoiceKind = OrderKind | Literal["resume", "sleep", "none"]


@dataclass(frozen=True, kw_only=True)
class ProjectAgenda:
    """The slice of a Project the agenda needs. `last_advance_at` None = never advanced."""

    id: ProjectId
    weight: int
    backlog: list[BacklogItem]
    last_advance_at: datetime | None
    goal: Goal
    default_profile: str


@dataclass(frozen=True, kw_only=True)
class AgendaState:
    pending_events: list[Event] = field(default_factory=list)  # unprocessed human messages
    closed_asks: list[AskOutcome] = field(default_factory=list)  # from Inbox.drain_closed()
    projects: list[ProjectAgenda] = field(default_factory=list)
    running_count: int = 0
    engine_available: bool = True
    next_reset: datetime
    last_discover_at: Mapping[ProjectId, datetime] = field(default_factory=dict)
    improve_rotation: list[str] = field(default_factory=lambda: list(IMPROVE_ROTATION))
    parallel_cap: int = PARALLEL_CAP
    # profile for project-less (improve) orders. Must be a real profile of the variant's
    # browser: WorkerPool.run raises UnknownProfile otherwise. The coordinator passes a
    # configured one.
    default_profile: str = "default"


@dataclass(frozen=True, kw_only=True)
class AgendaChoice:
    kind: ChoiceKind
    order: WorkOrder | None
    reason: str
    event: Event | None = None  # respond: the human message to handle
    resume: tuple[AskOutcome, ...] = ()  # resume: closed Asks to deliver
    improve_task: str | None = None  # improve: which entry of the rotation
    wake_at: datetime | None = None  # sleep: next engine reset
    skipped: tuple[str, ...] = ()  # first SKIPPED_MAX candidates passed over and why
    skipped_total: int = 0  # how many were passed over in all ("왜 이 일을 했나")

    def payload(self) -> dict:
        """`agenda.tick` event payload."""
        return {
            "kind": self.kind,
            "reason": self.reason,
            "project_id": self.order["project_id"] if self.order else None,
            "backlog_item_id": self.order["backlog_item_id"] if self.order else None,
            "improve_task": self.improve_task,
            "wake_at": self.wake_at.isoformat() if self.wake_at else None,
            "resume": [o["ask_id"] for o in self.resume],
            "skipped": list(self.skipped),
            "skipped_total": self.skipped_total,
        }


def _skips(skipped: list[str]) -> dict:
    return {"skipped": tuple(skipped[:SKIPPED_MAX]), "skipped_total": len(skipped)}


def has_goal_gap(goal: Goal) -> bool:
    """A KR still below its target. No KRs -> no measurable gap."""
    return any(kr["current"] < kr["target"] for kr in goal["key_results"])


def _is_starving(p: ProjectAgenda, now: datetime) -> bool:
    return p.last_advance_at is None or now - p.last_advance_at >= STARVING_AFTER


def _order(
    *,
    project_id: ProjectId | None,
    kind: OrderKind,
    intent: str,
    done_criteria: list[str],
    profile: str,
    priority: float,
    value: int,
    backlog_item_id: str | None = None,
) -> WorkOrder:
    return WorkOrder(
        id="",
        project_id=project_id,
        kind=kind,
        intent=intent[:INTENT_MAX],
        done_criteria=done_criteria[:CRITERIA_MAX],
        budget=Budget(max_minutes=DEFAULT_MAX_MINUTES),
        profile=profile,
        priority=priority,
        value=value,
        due=None,
        backlog_item_id=backlog_item_id,
        action_mode="guarded",
    )


def _choose_advance(state: AgendaState, now: datetime, skipped: list[str]) -> AgendaChoice | None:
    best: tuple[tuple, ProjectAgenda, BacklogItem, float, bool] | None = None
    for p in sorted(state.projects, key=lambda p: p.id):
        starving = _is_starving(p, now)
        for pos, it in enumerate(p.backlog):
            if it["status"] != "ready":
                continue
            if it["value"] < VALUE_THRESHOLD:
                skipped.append(f"{it['id']}: value {it['value']} < {VALUE_THRESHOLD}")
                continue
            score = it["value"] * p.weight / EST_COST
            key = (not starving, -score, p.id, pos)
            if best is None or key < best[0]:
                best = (key, p, it, score, starving)
    if best is None:
        return None
    _, p, it, score, starving = best
    reason = f"advance {p.id}/{it['id']}: value {it['value']} x weight {p.weight} = {score:g}"
    if starving:
        reason += f" (starving: no advance for {int(STARVING_AFTER.total_seconds() // 3600)}h+)"
    order = _order(
        project_id=p.id,
        kind="advance",
        intent=it["intent"] or it["title"],
        done_criteria=list(it["done_criteria"]) or [it["title"]],
        profile=p.default_profile,
        priority=score,
        value=it["value"],
        backlog_item_id=it["id"],
    )
    return AgendaChoice(kind="advance", order=order, reason=reason, **_skips(skipped))


def _discover_intent(goal: Goal) -> str:
    gaps = [
        f"- {kr['metric']}: {kr['current']:g} / {kr['target']:g}"
        for kr in goal["key_results"]
        if kr["current"] < kr["target"]
    ]
    return "\n".join(
        [
            f"목표 '{goal['objective']}'와의 간격을 줄일 기회를 찾는다.",
            "간격이 남은 KR:",
            *gaps,
            "할 일 후보를 근거(출처 URL)와 함께 제시한다. 실행은 하지 않는다.",
        ]
    )


def _choose_discover(state: AgendaState, now: datetime, skipped: list[str]) -> AgendaChoice | None:
    best: tuple[tuple, ProjectAgenda] | None = None
    for p in state.projects:
        if not has_goal_gap(p.goal):
            continue
        if any(it["status"] == "ready" for it in p.backlog):
            continue
        last = state.last_discover_at.get(p.id)
        if last is not None and now - last < DISCOVER_EVERY:
            skipped.append(f"{p.id}: discovered {last.isoformat()} (< 24h)")
            continue
        # starving first, then the longest since the last discover (never = oldest), then id
        key = (not _is_starving(p, now), last is not None, last or now, p.id)
        if best is None or key < best[0]:
            best = (key, p)
    if best is None:
        return None
    p = best[1]
    last = state.last_discover_at.get(p.id)
    since = "never" if last is None else last.isoformat()
    order = _order(
        project_id=p.id,
        kind="discover",
        intent=_discover_intent(p.goal),
        done_criteria=["할 일 후보를 근거와 함께 제시한다"],
        profile=p.default_profile,
        priority=0.0,
        value=VALUE_THRESHOLD,
    )
    return AgendaChoice(
        kind="discover",
        order=order,
        reason=f"discover {p.id}: goal gap, no ready items, last discover {since}",
        **_skips(skipped),
    )


def choose_next(state: AgendaState, now: datetime) -> AgendaChoice:
    # ① human message: always first, even with no engine available (Coordinator turn)
    if state.pending_events:
        ev = state.pending_events[0]
        return AgendaChoice(
            kind="respond", order=None, reason=f"human message {ev['id']}", event=ev
        )
    skipped: list[str] = [
        f"{o['ask_id']}: expired (run {o['run_id']} already ended)"
        for o in state.closed_asks
        if o["status"] == "expired"
    ]
    # ① Ask answered / ② reversible Ask defaulted -> resume the waiting runs
    live = tuple(o for o in state.closed_asks if o["status"] != "expired")
    if live:
        ids = ", ".join(o["ask_id"] for o in live)
        return AgendaChoice(
            kind="resume",
            order=None,
            reason=f"closed asks: {ids}",
            resume=live,
            **_skips(skipped),
        )
    # all engines resting -> sleep until the next reset (events still arrive)
    if not state.engine_available:
        return AgendaChoice(
            kind="sleep",
            order=None,
            reason=f"engine unavailable until {state.next_reset.isoformat()}",
            wake_at=state.next_reset,
            **_skips(skipped),
        )
    if state.running_count >= state.parallel_cap:
        return AgendaChoice(
            kind="none",
            order=None,
            reason=f"parallel cap reached ({state.running_count}/{state.parallel_cap})",
            **_skips(skipped),
        )
    for pick in (_choose_advance, _choose_discover):
        choice = pick(state, now, skipped)
        if choice is not None:
            return choice
    if state.improve_rotation:
        task = state.improve_rotation[0]
        order = _order(
            project_id=None,
            kind="improve",
            intent=f"improve: {task}",
            done_criteria=[f"{task} 완료"],
            profile=state.default_profile,
            priority=0.0,
            value=0,
        )
        return AgendaChoice(
            kind="improve",
            order=order,
            reason=f"no goal work worth doing; improve ({task}, oldest in rotation)",
            improve_task=task,
            **_skips(skipped),
        )
    return AgendaChoice(kind="none", order=None, reason="nothing to do", **_skips(skipped))
