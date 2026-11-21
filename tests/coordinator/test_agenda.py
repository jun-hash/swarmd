from datetime import datetime, timedelta

from swarm.clock import SEOUL
from swarm.coordinator.agenda import (
    IMPROVE_ROTATION,
    SKIPPED_MAX,
    VALUE_THRESHOLD,
    AgendaState,
    ProjectAgenda,
    choose_next,
    has_goal_gap,
)
from swarm.types import KR, AskOutcome, BacklogItem, Event, Goal

NOW = datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL)
_seq = iter(range(1, 10_000))


def tell(text="C사 무료 티어 영향 금요일까지", project_id="prj_a") -> Event:
    return Event(
        id=f"evt_{next(_seq)}",
        ts=NOW,
        kind="human.tell",
        actor="human",
        project_id=project_id,
        run_id=None,
        payload={"text": text, "channel": "telegram:-100123:42"},
        refs=[],
    )


def item(value=3, status="ready", title=None) -> BacklogItem:
    n = next(_seq)
    return BacklogItem(
        id=f"bl_{n}",
        title=title or f"item {n}",
        intent=f"intent {n}",
        done_criteria=[f"criterion {n}"],
        status=status,
        priority=0,
        value=value,
    )


def gap_goal() -> Goal:
    return Goal(
        objective="경쟁사 변화를 먼저 안다",
        key_results=[KR(id="kr_1", metric="주간 리포트", target=4, current=1, source="")],
    )


def met_goal() -> Goal:
    return Goal(
        objective="경쟁사 변화를 먼저 안다",
        key_results=[KR(id="kr_1", metric="주간 리포트", target=4, current=4, source="")],
    )


def project(
    pid="prj_a",
    *,
    backlog=(),
    weight=2,
    last_advance_at=NOW - timedelta(hours=1),
    goal=None,
) -> ProjectAgenda:
    return ProjectAgenda(
        id=pid,
        weight=weight,
        backlog=list(backlog),
        last_advance_at=last_advance_at,
        goal=goal if goal is not None else met_goal(),
        default_profile="work",
    )


def outcome(ask_id="ask_1", status="answered") -> AskOutcome:
    return AskOutcome(
        ask_id=ask_id, run_id="run_1", status=status, choice="B", raw_text="B로", via="telegram"
    )


def state(
    *,
    pending_events=(),
    closed_asks=(),
    backlog_ready=(),
    projects=None,
    running_count=0,
    engine_available=True,
    next_reset=NOW,
    last_discover_at=None,
    improve_rotation=IMPROVE_ROTATION,
) -> AgendaState:
    if projects is None:
        projects = [project(backlog=backlog_ready)]
    return AgendaState(
        pending_events=list(pending_events),
        closed_asks=list(closed_asks),
        projects=list(projects),
        running_count=running_count,
        engine_available=engine_available,
        next_reset=next_reset,
        last_discover_at=dict(last_discover_at or {}),
        improve_rotation=list(improve_rotation),
    )


# --- decision table ----------------------------------------------------------------------


def test_human_message_beats_everything():
    assert choose_next(state(pending_events=[tell()], backlog_ready=[item(value=5)]), NOW).kind == (
        "respond"
    )


def test_low_value_backlog_is_skipped_then_improve():
    """AC-2: below the value threshold nothing is dispatched; the agenda improves instead."""
    c = choose_next(state(backlog_ready=[item(value=2)], engine_available=True), NOW)
    assert c.kind == "improve" and c.order["project_id"] is None  # 사람 알림 대상 아님


def test_discover_when_gap_and_no_ready_items_and_24h_since_last():
    p = project(backlog=[item(value=5, status="triage")], goal=gap_goal())
    old = {"prj_a": NOW - timedelta(hours=25)}
    c = choose_next(state(projects=[p], last_discover_at=old), NOW)
    assert c.kind == "discover"
    assert c.order["project_id"] == "prj_a" and c.order["kind"] == "discover"
    # discovered 23h ago -> not yet; nothing else to do -> improve
    recent = {"prj_a": NOW - timedelta(hours=23)}
    assert choose_next(state(projects=[p], last_discover_at=recent), NOW).kind == "improve"
    # a ready item (even low value) means "not empty" -> no discover
    p2 = project(backlog=[item(value=2)], goal=gap_goal())
    assert choose_next(state(projects=[p2], last_discover_at=old), NOW).kind == "improve"
    # no gap -> no discover
    p3 = project(backlog=[], goal=met_goal())
    assert choose_next(state(projects=[p3], last_discover_at=old), NOW).kind == "improve"


def test_never_discovered_project_with_gap_discovers():
    p = project(backlog=[], goal=gap_goal())
    assert choose_next(state(projects=[p]), NOW).kind == "discover"


def test_starving_project_gets_priority_after_48h():
    rich = project("prj_rich", backlog=[item(value=5)], weight=3)
    poor_item = item(value=3)
    poor = project(
        "prj_poor", backlog=[poor_item], weight=1, last_advance_at=NOW - timedelta(hours=49)
    )
    c = choose_next(state(projects=[rich, poor]), NOW)
    assert c.kind == "advance"
    assert c.order["project_id"] == "prj_poor"
    assert c.order["backlog_item_id"] == poor_item["id"]
    assert "48" in c.reason
    # 47h without advance is not starving: score wins
    poor_ok = project(
        "prj_poor", backlog=[poor_item], weight=1, last_advance_at=NOW - timedelta(hours=47)
    )
    assert choose_next(state(projects=[rich, poor_ok]), NOW).order["project_id"] == "prj_rich"


def test_sleep_when_all_engines_cooling():
    reset = NOW + timedelta(hours=2)
    c = choose_next(
        state(backlog_ready=[item(value=5)], engine_available=False, next_reset=reset), NOW
    )
    assert c.kind == "sleep" and c.order is None and c.wake_at == reset


# --- more ---------------------------------------------------------------------------


def test_human_message_wins_even_when_engine_cooling():
    ev = tell()
    c = choose_next(state(pending_events=[ev], engine_available=False), NOW)
    assert c.kind == "respond" and c.event == ev and c.order is None


def test_closed_asks_resume_before_goal_work_even_when_cooling():
    outs = [outcome("ask_1"), outcome("ask_2", status="defaulted")]
    c = choose_next(
        state(closed_asks=outs, backlog_ready=[item(value=5)], engine_available=False), NOW
    )
    assert c.kind == "resume" and c.order is None and list(c.resume) == outs


def test_human_message_beats_closed_asks():
    c = choose_next(state(pending_events=[tell()], closed_asks=[outcome()]), NOW)
    assert c.kind == "respond"


def test_advance_picks_highest_value_times_weight():
    a = item(value=5)  # 5 x 1 = 5
    b = item(value=4)  # 4 x 2 = 8
    pa = project("prj_a", backlog=[a], weight=1)
    pb = project("prj_b", backlog=[b], weight=2)
    c = choose_next(state(projects=[pa, pb]), NOW)
    assert c.kind == "advance" and c.order["backlog_item_id"] == b["id"]
    o = c.order
    assert o["project_id"] == "prj_b" and o["kind"] == "advance"
    assert o["intent"] == b["intent"] and o["done_criteria"] == b["done_criteria"]
    assert o["value"] == 4 and o["profile"] == "work" and o["action_mode"] == "guarded"
    assert o["budget"]["max_minutes"] == 60


def test_value_threshold_is_three():
    assert VALUE_THRESHOLD == 3
    c = choose_next(state(backlog_ready=[item(value=3)]), NOW)
    assert c.kind == "advance"


def test_only_ready_items_are_advanced():
    c = choose_next(state(backlog_ready=[item(value=5, status="todo")]), NOW)
    assert c.kind == "improve"


def test_parallel_cap_reached_waits():
    c = choose_next(state(backlog_ready=[item(value=5)], running_count=3), NOW)
    assert c.kind == "none" and c.order is None


def test_improve_takes_oldest_in_rotation():
    assert IMPROVE_ROTATION == ["replay_failed", "memory_consolidate"]
    c = choose_next(state(projects=[]), NOW)
    assert c.kind == "improve" and c.improve_task == "replay_failed"
    assert c.order["kind"] == "improve" and c.order["project_id"] is None
    c2 = choose_next(state(projects=[], improve_rotation=["memory_consolidate"]), NOW)
    assert c2.improve_task == "memory_consolidate"


def test_nothing_to_improve_is_none():
    assert choose_next(state(projects=[], improve_rotation=[]), NOW).kind == "none"


def test_low_value_items_are_recorded_as_skipped():
    low = item(value=2, title="별로인 일")
    c = choose_next(state(backlog_ready=[low]), NOW)
    assert any(low["id"] in s for s in c.skipped)
    assert c.payload()["reason"] == c.reason and c.payload()["kind"] == "improve"


def test_choose_next_is_deterministic():
    s = state(projects=[project("prj_a", backlog=[item(5)]), project("prj_b", backlog=[item(5)])])
    assert choose_next(s, NOW) == choose_next(s, NOW)
    assert choose_next(s, NOW).order["project_id"] == "prj_a"  # tie -> project id order


def test_has_goal_gap():
    assert has_goal_gap(gap_goal()) and not has_goal_gap(met_goal())
    assert not has_goal_gap(Goal(objective="x", key_results=[]))


def test_expired_asks_are_not_resumed():
    c = choose_next(state(closed_asks=[outcome("ask_x", status="expired")]), NOW)
    assert c.kind == "improve" and not c.resume
    assert any("ask_x" in s and "expired" in s for s in c.skipped)
    mixed = [outcome("ask_x", status="expired"), outcome("ask_ok")]
    c2 = choose_next(state(closed_asks=mixed), NOW)
    assert c2.kind == "resume" and [o["ask_id"] for o in c2.resume] == ["ask_ok"]
    assert any("ask_x" in s for s in c2.skipped)


def test_skipped_is_capped_with_total():
    many = [item(value=1) for _ in range(30)]
    c = choose_next(state(backlog_ready=many), NOW)
    assert len(c.skipped) == SKIPPED_MAX == 20 and c.skipped_total == 30
    p = c.payload()
    assert len(p["skipped"]) == 20 and p["skipped_total"] == 30
