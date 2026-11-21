"""Coordinator: agenda loop, resume, tell handling, kanban, reports."""

import asyncio
import json
from collections.abc import Callable
from datetime import datetime, time, timedelta

import pytest
from fakes import NotifierSpy

from swarm.clock import SEOUL, FakeClock
from swarm.coordinator.conversation import FAILED_ACK, HELD_BACKOFF, INTERIM_ACK
from swarm.coordinator.coordinator import Coordinator
from swarm.coordinator.dispatch import ITEM_BACKOFF, ITEM_STOP_AFTER
from swarm.coordinator.messages import Candidate, Candidates, MessageClass
from swarm.coordinator.reports import REPORT_BACKOFF
from swarm.coordinator.resumes import RESUME_BACKOFF
from swarm.inbox.inbox import SqliteInbox
from swarm.ports import BrainError
from swarm.projects.store import SqliteProjectStore
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import (
    KR,
    AskDraft,
    AskOutcome,
    BacklogItem,
    Event,
    Project,
    RunResult,
    TakeoverInfo,
    UsageSignal,
    Variant,
    WorkerRun,
)
from swarm.workers.contract import IllegalTransition, validate_order

NOW = datetime(2026, 11, 5, 3, 0, tzinfo=SEOUL)
FAILED_ACK_HEAD = FAILED_ACK[:5]  # Thursday, before the 08:00 briefing
CH = "telegram:-100123:42"
GENERAL = "telegram:-100123:1"
V = Variant(harness="own", engine="claude", browser="chrome", context_policy="default", model=None)


def make_project(krs: list[KR] | None = None) -> Project:
    return Project(
        id="prj_compet",
        slug="compet",
        name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": krs or []},
        strategy="",
        policies=[],
        autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[],
        artifacts=[],
        channel=CH,
        board="compet",
        default_profile="work",
        default_variant=V,
        weight=2,
        watches=[],
    )


def mc(**kw) -> MessageClass:
    base = dict(
        category="new_work",
        target_ask_number=None,
        target_run_hint=None,
        summary="C사 무료 티어 영향",
        due=None,
        confidence=0.9,
    )
    base.update(kw)
    return MessageClass(**base)


def draft(question="어느 안으로 갈까?", **kw) -> AskDraft:
    d = AskDraft(
        kind="decision",
        question=question,
        options=["A", "B"],
        recommended="B",
        default_after=None,
        irreversible=False,
        evidence=[],
    )
    d.update(kw)  # type: ignore[typeddict-item]
    return d


def result(run_id: str, status="done", **kw) -> RunResult:
    r = RunResult(
        run_id=run_id,
        status=status,
        artifacts=[],
        summary="끝",
        cost={"turns": 1, "minutes": 1.0, "engine_usage": {}},
        failure=None,
    )
    r.update(kw)  # type: ignore[typeddict-item]
    return r


class ScriptBrain:
    """Per-purpose scripted results; an Exception value is raised."""

    def __init__(self):
        self.by_purpose: dict[str, object] = {}
        self.calls: list[dict] = []
        self.delay = 0.0

    def returns(self, value, purpose="classify_message"):
        self.by_purpose[purpose] = value

    async def decide(self, *, purpose, prompt, schema, timeout_s=60):
        self.calls.append({"purpose": purpose, "prompt": prompt, "schema": schema})
        if self.delay:
            await asyncio.sleep(self.delay)
        r = self.by_purpose.get(purpose)
        if r is None:
            raise BrainError("no result")
        if isinstance(r, Exception):
            raise r
        return r


class FakePool:
    """Records calls; `run` holds a fake active row and releases the lease like the pool."""

    def __init__(self, ledger, clock):
        self.ledger = ledger
        self.clock = clock
        self.runs: list[dict] = []
        self.resumes: list[dict] = []
        self.calls: list[tuple] = []
        self.active: dict[str, WorkerRun] = {}
        self.project_of: dict[str, str | None] = {}
        self.result: Callable[[dict, str], RunResult] = lambda order, rid: result(rid)
        self.gate: asyncio.Event | None = None
        self.resume_errors: list[Exception] = []
        self.during = None  # async (order, run_id) hook inside run(), e.g. raise an Ask
        self.ended: dict[str, WorkerRun] = {}  # finished drives: get() still knows them

    def add_active(self, run_id, project_id, status="running", order_id="wo_x"):
        self.project_of[run_id] = project_id
        self.active[run_id] = WorkerRun(
            id=run_id,
            order_id=order_id,
            variant=V,
            status=status,
            started_at=self.clock.now(),
            ended_at=None,
            human_intervened=False,
        )

    async def run(self, order, variant, *, lease):
        validate_order(order)
        run_id = f"run_{len(self.runs) + 1}"
        self.runs.append({"order": order, "variant": variant, "lease": lease, "run_id": run_id})
        self.add_active(run_id, order["project_id"], order_id=order["id"])
        res = None
        try:
            if self.gate is not None:
                await self.gate.wait()
            if self.during is not None:
                await self.during(order, run_id)
            res = self.result(order, run_id)
            return res
        finally:
            run = self.active.pop(run_id, None)
            if run is not None and res is not None:
                self.ended[run_id] = WorkerRun(**{**run, "status": res["status"]})
            if lease is not None:
                self.ledger.release(lease)

    async def resume(self, run_id, *, outcome, lease):
        self.resumes.append({"run_id": run_id, "outcome": outcome, "lease": lease})
        if lease is not None:
            self.ledger.release(lease)
        if self.resume_errors:
            raise self.resume_errors.pop(0)
        return result(run_id)

    async def wait(self, run_id):
        self.calls.append(("wait", run_id))
        return result(run_id)

    async def steer(self, run_id, *, text):
        self.calls.append(("steer", run_id, text))
        return "at_tool_boundary"

    async def takeover(self, run_id):
        self.calls.append(("takeover", run_id))
        return TakeoverInfo(run_id=run_id, profile="work", cdp_url="ws://x", target_id="t1")

    async def release(self, run_id, *, note):
        self.calls.append(("release", run_id, note))

    async def cancel(self, run_id, *, reason):
        self.calls.append(("cancel", run_id, reason))

    def running(self):
        return list(self.active.values())

    def run_project(self, run_id):
        return self.project_of.get(run_id)

    def get(self, run_id):
        if run_id in self.active:
            return self.active[run_id]
        if run_id in self.ended:
            return self.ended[run_id]
        raise KeyError(run_id)


# Overrides the root clock: tests here count from the module's NOW.
@pytest.fixture
def clock():
    return FakeClock(NOW)


@pytest.fixture
def fake_brain():
    return ScriptBrain()


@pytest.fixture
def notifier():
    return NotifierSpy()


@pytest.fixture
def stack(tmp_path, db, log, clock, fake_brain, notifier):
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    pool = FakePool(ledger, clock)
    holder: dict = {}
    inbox = SqliteInbox(
        db,
        log,
        projects,
        fake_brain,
        clock,
        run_project=lambda r: holder["c"].ask_project(r) or pool.run_project(r),
        general_channel=GENERAL,
    )

    def make(**kw) -> Coordinator:
        c = Coordinator(
            db=db,
            log=log,
            projects=projects,
            inbox=kw.pop("inbox", inbox),
            ledger=ledger,
            pool=pool,
            brain=fake_brain,
            notifier=kw.pop("notifier", notifier),
            clock=clock,
            general_channel=GENERAL,
            default_profile="work",
            default_variant=V,
            **kw,
        )
        holder["c"] = c
        return c

    return {
        "db": db,
        "log": log,
        "projects": projects,
        "ledger": ledger,
        "pool": pool,
        "inbox": inbox,
        "make": make,
    }


@pytest.fixture
def projects(stack):
    return stack["projects"]


@pytest.fixture
def inbox(stack):
    return stack["inbox"]


@pytest.fixture
def pool_spy(stack):
    return stack["pool"]


@pytest.fixture
def pid(projects):
    return projects.create(make_project())


@pytest.fixture
async def coordinator(stack, pid):
    c = stack["make"]()
    yield c
    await c.aclose()


def kinds(log, kind):
    return [e for e in log.query(kinds=[kind])]


def ready_item(projects, pid, title="C사 가격표 정리", value=4) -> str:
    return projects.add_backlog(
        pid,
        BacklogItem(
            title=title,
            intent=title,
            done_criteria=[title],
            status="ready",
            priority=0,
            value=value,
        ),
    )


# --- loop / tick ----------------------------------------------------------------------


async def test_tick_records_choice_and_reason(coordinator, log):
    await coordinator.tick()
    ticks = kinds(log, "agenda.tick")
    assert ticks and all(t["payload"]["reason"] for t in ticks)
    assert ticks[0]["payload"]["kind"] in ("improve", "none")


async def test_advance_order_gets_real_id_and_result_is_recorded(
    coordinator, projects, pid, pool_spy, log
):
    bid = ready_item(projects, pid)
    art = {
        "id": "art_1",
        "title": "가격표",
        "path": "/tmp/a.md",
        "status": "verified",
        "evidence": [],
        "verifier_run_id": "run_v",
    }
    pool_spy.result = lambda order, rid: result(rid, artifacts=[art])
    await coordinator.tick()
    await coordinator.drain()
    [run] = pool_spy.runs
    o = run["order"]
    assert o["id"].startswith("wo_") and o["backlog_item_id"] == bid and o["kind"] == "advance"
    assert run["variant"] == V and run["lease"]["purpose"] == "advance"
    p = projects.get(pid)
    assert p["artifacts"][0]["id"] == "art_1"
    assert [b["status"] for b in p["backlog"]] == ["done"]
    [d] = kinds(log, "agenda.dispatched")
    assert d["payload"]["order_id"] == o["id"] and d["payload"]["kind"] == "advance"


async def test_failed_run_blocks_item_and_tells_project(
    coordinator, projects, pid, pool_spy, notifier
):
    ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(
        rid, status="failed", failure={"what": "가격표", "why": "로그인 벽", "run_id": rid}
    )
    await coordinator.tick()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "blocked"
    [m] = notifier.sent
    assert m["channel"] == CH and m["kind"] == "fyi" and "로그인 벽" in m["text"]


async def test_parallel_work_cap_leaves_a_slot_for_the_coordinator(
    coordinator, projects, pid, pool_spy
):
    pool_spy.gate = asyncio.Event()
    for i in range(4):
        ready_item(projects, pid, title=f"일 {i}")
    await coordinator.tick()
    await asyncio.sleep(0)
    assert len(pool_spy.runs) == 2  # work_cap 2 of claude's 3: Brain / resume keep one
    pool_spy.gate.set()
    await coordinator.drain()


async def test_submit_wakes_loop_and_updates_heartbeat(coordinator, clock, log):
    stop = asyncio.Event()
    task = asyncio.create_task(coordinator.agenda_loop(stop))
    await asyncio.sleep(0.05)
    n1 = len(kinds(log, "agenda.tick"))
    assert n1 >= 1
    clock.advance(timedelta(minutes=5))
    await coordinator.submit(
        Event(
            id="",
            ts=clock.now(),
            kind="run.finished",
            actor="workers",
            project_id=None,
            run_id="run_x",
            payload={},
            refs=[],
        )
    )
    await asyncio.sleep(0.05)
    assert len(kinds(log, "agenda.tick")) > n1
    assert coordinator.heartbeat_at == clock.now()
    stop.set()
    await asyncio.wait_for(task, 1)


async def test_tick_error_is_logged_and_loop_continues(coordinator, projects, log, monkeypatch):
    def boom():
        raise RuntimeError("disk")

    monkeypatch.setattr(projects, "ingest_file_edits", boom)
    stop = asyncio.Event()
    task = asyncio.create_task(coordinator.agenda_loop(stop))
    await asyncio.sleep(0.05)
    await coordinator.submit(
        Event(
            id="",
            ts=NOW,
            kind="coordinator.wake",
            actor="test",
            project_id=None,
            run_id=None,
            payload={},
            refs=[],
        )
    )
    await asyncio.sleep(0.05)
    errs = kinds(log, "agenda.tick_error")
    assert len(errs) >= 2 and errs[0]["payload"]["phase"] == "ingest"
    assert "disk" in errs[0]["payload"]["error"]
    assert kinds(log, "agenda.tick")  # the rest of the tick still ran
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, 1)


async def test_aclose_cancels_and_gathers_tracked_tasks(
    coordinator, projects, pid, pool_spy, stack
):
    pool_spy.gate = asyncio.Event()  # never released
    ready_item(projects, pid)
    await coordinator.tick()
    await asyncio.sleep(0)
    assert coordinator.task_count() == 1
    await coordinator.aclose()
    assert coordinator.task_count() == 0
    assert stack["ledger"].lease(engine_pref=["claude"], purpose="advance") is not None


# --- resume ----------------------------------------------------------------------------


async def answered(inbox, pool_spy, pid, run_id="run_9", choice="B") -> AskOutcome:
    pool_spy.add_active(run_id, pid, status="waiting_human")
    t = await inbox.raise_ask(run_id, draft())
    return await inbox.answer(t["ask_id"], choice=choice, raw_text=choice, via=CH)


async def test_closed_asks_are_resumed_with_respond_lease(coordinator, inbox, pool_spy, pid):
    out = await answered(inbox, pool_spy, pid)
    await coordinator.tick()
    await coordinator.drain()
    [r] = pool_spy.resumes
    assert r["run_id"] == "run_9" and r["outcome"]["ask_id"] == out["ask_id"]
    assert r["outcome"]["choice"] == "B" and r["lease"]["purpose"] == "respond"
    await coordinator.tick()
    await coordinator.drain()
    assert len(pool_spy.resumes) == 1  # exactly once


async def test_resume_without_lease_is_kept_and_survives_restart(
    stack, coordinator, inbox, pool_spy, pid, clock, log
):
    stack["ledger"].observe(
        engine="claude",
        signal=UsageSignal(kind="limit_hit", reset_at=NOW + timedelta(hours=1), at=NOW),
    )
    await answered(inbox, pool_spy, pid)
    await coordinator.tick()
    await coordinator.drain()
    assert pool_spy.resumes == []
    assert kinds(log, "coordinator.resume_deferred")
    await coordinator.aclose()
    clock.advance(timedelta(hours=2))
    c2 = stack["make"]()  # restart: the drained outcome is still pending
    await c2.tick()
    await c2.drain()
    assert [r["run_id"] for r in pool_spy.resumes] == ["run_9"]
    await c2.aclose()


async def test_resume_error_keeps_outcome_for_a_later_tick(
    coordinator, inbox, pool_spy, pid, clock
):
    pool_spy.resume_errors = [IllegalTransition("changed while resuming")]
    await answered(inbox, pool_spy, pid)
    await coordinator.tick()
    await coordinator.drain()
    clock.advance(timedelta(seconds=61))  # first backoff step
    await coordinator.tick()
    await coordinator.drain()
    assert [r["run_id"] for r in pool_spy.resumes] == ["run_9", "run_9"]
    await coordinator.tick()
    await coordinator.drain()
    assert len(pool_spy.resumes) == 2  # succeeded the second time: no more retries


async def test_expired_outcome_is_logged_never_resumed(stack, pid, pool_spy, log):
    class ExpiringInbox:
        def __init__(self, inner):
            self._inner = inner
            self.once = True

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def drain_closed(self):
            out = self._inner.drain_closed()
            if self.once:
                self.once = False
                out.append(
                    AskOutcome(
                        ask_id="ask_old",
                        run_id="run_gone",
                        status="expired",
                        choice=None,
                        raw_text=None,
                        via="expired",
                    )
                )
            return out

    c = stack["make"](inbox=ExpiringInbox(stack["inbox"]))
    await c.tick()
    await c.drain()
    assert pool_spy.resumes == []
    [e] = kinds(log, "coordinator.ask_expired")
    assert e["payload"]["ask_id"] == "ask_old" and e["run_id"] == "run_gone"
    await c.aclose()


# --- tell -------------------------------------------------------------------------------


async def test_tell_new_work_creates_backlog_and_acks_within_10s(
    coordinator, fake_brain, clock, projects, pid, pool_spy
):
    fake_brain.returns(
        MessageClass(
            category="new_work",
            summary="C사 무료 티어 영향",
            due="2026-11-12T18:00",
            confidence=0.9,
            target_ask_number=None,
            target_run_hint=None,
        )
    )
    r = await asyncio.wait_for(
        coordinator.handle_tell("telegram:-100123:42", "C사 무료 티어 영향 금요일까지"), 10
    )
    assert r["order_id"] and "까지" in r["ack"]
    assert r["project_id"] == pid and "11/12" in r["ack"] and "18:00" in r["ack"]
    [item] = projects.get(pid)["backlog"]
    assert item["title"] == "C사 무료 티어 영향" and item["status"] == "ready"
    await coordinator.tick()  # human order goes first, under the id the human was given
    await coordinator.drain()
    [run] = pool_spy.runs
    assert run["order"]["id"] == r["order_id"] and run["order"]["backlog_item_id"] == item["id"]
    assert run["order"]["due"] == datetime(2026, 11, 12, 18, 0, tzinfo=SEOUL)
    assert "C사 무료 티어 영향 금요일까지" in run["order"]["intent"]


async def test_new_work_without_due_acks_an_estimate_rounded_up(
    coordinator, fake_brain, clock, pool_spy
):
    clock.advance(timedelta(minutes=10))  # 03:10 + 60 min budget = 04:10 -> 04:30
    fake_brain.returns(mc(summary="C사 가격표"))
    r = await coordinator.handle_tell(CH, "C사 가격표 봐줘")
    assert r["ack"] == "받았어요. 11/5(목) 04:30쯤까지 드릴게요: C사 가격표"
    await coordinator.tick()
    await coordinator.drain()
    assert pool_spy.runs[0]["order"]["due"] is None  # an estimate, not a commitment


async def test_new_work_eta_on_a_half_hour_is_kept(coordinator, fake_brain):
    fake_brain.returns(mc(summary="C사 가격표"))  # 03:00 + 60 = 04:00 exactly
    r = await coordinator.handle_tell(CH, "C사 가격표 봐줘")
    assert "11/5(목) 04:00쯤까지" in r["ack"]


async def test_new_work_eta_starts_at_the_reset_when_the_engine_is_limited(
    coordinator, fake_brain, clock, stack
):
    clock.advance(timedelta(minutes=10))
    stack["ledger"].observe(
        engine="claude",
        signal=UsageSignal(kind="limit_hit", reset_at=NOW + timedelta(hours=2, minutes=10),
                           at=clock.now()),
    )  # fmt: skip
    fake_brain.returns(mc(summary="C사 가격표"))
    r = await coordinator.handle_tell(CH, "C사 가격표 봐줘")
    assert "11/5(목) 06:30쯤까지" in r["ack"]  # reset 05:10 + 60 = 06:10 -> 06:30


async def test_new_work_with_due_keeps_the_committed_deadline(coordinator, fake_brain, clock):
    clock.advance(timedelta(minutes=10))
    fake_brain.returns(mc(summary="C사 가격표", due="2026-11-12T18:00"))
    r = await coordinator.handle_tell(CH, "C사 가격표 금요일까지")
    assert r["ack"] == "받았어요. 11/12(목) 18:00까지 드릴게요."


async def test_new_work_done_is_reported_to_the_human(coordinator, fake_brain, pool_spy, notifier):
    fake_brain.returns(mc())
    await coordinator.handle_tell(CH, "C사 무료 티어 영향 봐줘")
    pool_spy.result = lambda order, rid: result(rid, summary="C사는 무료 티어를 없앴다")
    await coordinator.tick()
    await coordinator.drain()
    [m] = notifier.sent
    assert m["channel"] == CH and "C사는 무료 티어를 없앴다" in m["text"]


async def test_new_work_in_general_asks_for_the_project(coordinator, fake_brain, projects, pid):
    fake_brain.returns(mc())
    r = await coordinator.handle_tell(GENERAL, "C사 봐줘")
    assert r["order_id"] is None and r["project_id"] is None and "프로젝트" in r["ack"]
    assert projects.get(pid)["backlog"] == []


async def test_slow_classification_returns_interim_ack(stack, pid, fake_brain, notifier, projects):
    c = stack["make"](ack_timeout_s=0.05)
    fake_brain.returns(mc(due="2026-11-12T18:00"))
    fake_brain.delay = 0.3
    r = await c.handle_tell(CH, "C사 무료 티어 영향 금요일까지")
    assert r["ack"] == INTERIM_ACK and r["order_id"] is None
    await c.drain()  # finished in the background, real ack via the notifier
    [m] = notifier.sent
    assert m["kind"] == "ack" and m["channel"] == CH and "까지" in m["text"]
    assert len(projects.get(pid)["backlog"]) == 1
    await c.aclose()


async def test_held_tell_backs_off_and_tells_the_human_only_at_the_end(
    coordinator, fake_brain, notifier, clock
):
    fake_brain.returns(BrainError("engine unavailable"))
    r = await coordinator.handle_tell(CH, "C사 봐줘")
    assert r["ack"] == INTERIM_ACK
    await coordinator.tick()  # held: not retried within milliseconds
    await coordinator.drain()
    assert len(fake_brain.calls) == 1
    for n, delay in enumerate((60, 300, 900), start=2):
        clock.advance(timedelta(seconds=delay - 5))
        await coordinator.tick()
        await coordinator.drain()
        assert len(fake_brain.calls) == n - 1
        clock.advance(timedelta(seconds=10))
        await coordinator.tick()
        await coordinator.drain()
        assert len(fake_brain.calls) == n
        if n < 4:
            assert notifier.sent == []  # no "다시 보내 주세요" while retries remain
    [m] = notifier.sent
    assert m["kind"] == "fyi" and m["channel"] == CH and "C사 봐줘" in m["text"]
    assert {c["purpose"] for c in fake_brain.calls} == {"classify_message"}


async def test_brain_failure_retry_success_sends_the_ack(coordinator, fake_brain, notifier, clock):
    fake_brain.returns(BrainError("engine unavailable"))
    await coordinator.handle_tell(CH, "C사 봐줘")
    fake_brain.returns(mc())
    clock.advance(timedelta(seconds=61))
    await coordinator.tick()
    await coordinator.drain()
    [m] = [m for m in notifier.sent if m["kind"] == "ack"]
    assert "받았어요" in m["text"]


async def test_goal_change_by_message_asks_confirmation(
    coordinator, fake_brain, projects, pid, inbox, notifier, pool_spy, log
):
    fake_brain.returns(mc(category="goal_change", summary="추적 경쟁사 5곳 -> 8곳"))
    r = await coordinator.handle_tell(CH, "경쟁사 8곳으로 늘리자")
    assert projects.get(pid)["goal"]["objective"] == "경쟁사 변화를 먼저 안다"  # not applied
    [ask] = inbox.open_asks(pid)
    assert "추적 경쟁사 5곳 -> 8곳" in ask["question"] and ask["options"] == ["바꿔요", "그대로"]
    assert ask["irreversible"]  # never closed by a policy or a default: goals are the human's
    [m] = notifier.sent
    assert m["kind"] == "ask" and m["ref_id"] == ask["ask_id"] and m["channel"] == CH
    assert "#1" in r["ack"] and r["order_id"] is None
    # the human confirms: the outcome is the Coordinator's own, never a pool resume
    fake_brain.returns(mc(category="ask_answer", target_ask_number=1))
    await coordinator.handle_tell(CH, "#1 바꿔요")
    await coordinator.tick()
    await coordinator.drain()
    assert pool_spy.resumes == []
    [e] = kinds(log, "coordinator.change_confirmed")
    assert e["payload"]["summary"] == "추적 경쟁사 5곳 -> 8곳"
    assert "GOAL.md" in notifier.sent[-1]["text"]


async def test_strategy_change_confirmed_becomes_a_policy(coordinator, fake_brain, projects, pid):
    fake_brain.returns(mc(category="strategy_change", summary="요금제 페이지만 본다"))
    await coordinator.handle_tell(CH, "앞으로 요금제 페이지만 보자")
    assert projects.get(pid)["policies"] == []
    fake_brain.returns(mc(category="ask_answer", target_ask_number=1))
    await coordinator.handle_tell(CH, "#1 바꿔요")
    await coordinator.tick()
    await coordinator.drain()
    [pol] = projects.get(pid)["policies"]
    assert "요금제 페이지만 본다" in pol["text"] and pol["source_ask"]


async def two_asks(inbox, pool_spy, pid, **kw):
    pool_spy.add_active("run_1", pid, status="waiting_human")
    pool_spy.add_active("run_2", pid, status="waiting_human")
    a1 = await inbox.raise_ask("run_1", draft("로그인 계정은?", **kw))
    a2 = await inbox.raise_ask("run_2", draft("C사 대응안은?", **kw))
    return a1["ask_id"], a2["ask_id"]


async def test_ask_answer_by_number_closes_that_ask(coordinator, inbox, fake_brain, pool_spy, pid):
    a1, a2 = await two_asks(inbox, pool_spy, pid)
    assert (inbox.topic_number(a1), inbox.topic_number(a2)) == (1, 2)
    fake_brain.returns(mc(category="ask_answer", target_ask_number=2, summary="B"))
    r = await coordinator.handle_tell(CH, "#2 B로")
    assert r["closed_ask_id"] == a2 and "B" in r["ack"]
    assert [a["ask_id"] for a in inbox.open_asks(pid)] == [a1]
    [o] = inbox.drain_closed()
    assert o["ask_id"] == a2 and o["choice"] == "B" and o["raw_text"] == "#2 B로"


async def test_ambiguous_answer_with_two_open_asks_reasks(
    coordinator, inbox, fake_brain, pool_spy, pid
):
    await two_asks(inbox, pool_spy, pid)
    fake_brain.returns(mc(category="ask_answer", target_ask_number=None, confidence=0.5))
    r = await coordinator.handle_tell(CH, "B로")
    assert r["closed_ask_id"] is None
    assert "#1" in r["ack"] and "#2" in r["ack"]
    assert len(inbox.open_asks(pid)) == 2


async def test_answer_targets_only_asks_of_the_same_channel(
    coordinator, inbox, fake_brain, pool_spy, pid
):
    pool_spy.add_active("run_free", None, status="waiting_human")  # no project -> General #1
    general = await inbox.raise_ask("run_free", draft("전역 질문?"))
    pool_spy.add_active("run_1", pid, status="waiting_human")
    mine = await inbox.raise_ask("run_1", draft("프로젝트 질문?"))
    fake_brain.returns(mc(category="ask_answer", target_ask_number=None, confidence=0.9))
    r = await coordinator.handle_tell(CH, "A로")
    assert r["closed_ask_id"] == mine["ask_id"]
    assert [a["ask_id"] for a in inbox.open_asks()] == [general["ask_id"]]


async def test_approval_answer_needs_the_exact_option(
    coordinator, inbox, fake_brain, pool_spy, pid
):
    pool_spy.add_active("run_1", pid, status="waiting_human")
    t = await inbox.raise_ask(
        "run_1",
        draft("C사 폼 제출할까?", kind="approval", options=["승인", "거절"], recommended=None),
    )
    fake_brain.returns(mc(category="ask_answer", target_ask_number=1))
    fake_brain.returns(BrainError("must not be asked"), purpose="map_answer")
    r = await coordinator.handle_tell(CH, "#1 승인 안 해")
    assert r["closed_ask_id"] is None and "승인" in r["ack"] and "거절" in r["ack"]
    r = await coordinator.handle_tell(CH, "#1 거절")
    assert r["closed_ask_id"] == t["ask_id"]
    assert all(c["purpose"] != "map_answer" for c in fake_brain.calls)


async def test_free_text_answer_is_mapped_by_brain(coordinator, inbox, fake_brain, pool_spy, pid):
    from swarm.coordinator.messages import AnswerChoice

    pool_spy.add_active("run_1", pid, status="waiting_human")
    t = await inbox.raise_ask("run_1", draft(options=["회사 계정", "개인 계정"]))
    fake_brain.returns(mc(category="ask_answer"))
    fake_brain.returns(AnswerChoice(choice="개인 계정", confidence=0.9), purpose="map_answer")
    r = await coordinator.handle_tell(CH, "내 걸로 해")
    assert r["closed_ask_id"] == t["ask_id"] and "개인 계정" in r["ack"]


async def test_policy_feedback_and_status(coordinator, fake_brain, projects, pid):
    fake_brain.returns(mc(category="policy_add", summary="액션엔 담당자·기한"))
    r = await coordinator.handle_tell(CH, "앞으로 액션엔 담당자랑 기한 붙여")
    assert "정책 +1" in r["ack"]
    assert projects.get(pid)["policies"][0]["text"] == "앞으로 액션엔 담당자랑 기한 붙여"
    fake_brain.returns(mc(category="feedback", summary="리포트가 길다"))
    await coordinator.handle_tell(CH, "리포트가 너무 길어")
    [lrn] = projects.get(pid)["memory"]["learnings"]
    assert (
        "리포트가 너무 길어" in lrn["text"] and lrn["evidence"][0]["text"] == "리포트가 너무 길어"
    )
    fake_brain.returns(mc(category="status"))
    r = await coordinator.handle_tell(CH, "요즘 어때?")
    assert "경쟁사" in r["ack"] and len(r["ack"]) <= 3500


async def test_steer_takeover_release_route_to_the_run(
    coordinator, fake_brain, pool_spy, pid, projects
):
    pool_spy.add_active("run_7", pid)
    fake_brain.returns(mc(category="steer", target_run_hint=None))
    r = await coordinator.handle_tell(CH, "C사 건은 요금제 페이지만 봐")
    assert ("steer", "run_7", "C사 건은 요금제 페이지만 봐") in pool_spy.calls
    assert "다음 단계부터" in r["ack"]
    fake_brain.returns(mc(category="takeover"))
    r = await coordinator.handle_tell(CH, "내가 B사 로그인 직접 할게")
    assert ("takeover", "run_7") in pool_spy.calls and "됐어" in r["ack"]
    pool_spy.active["run_7"]["status"] = "waiting_human"
    fake_brain.returns(mc(category="release"))
    await coordinator.handle_tell(CH, "됐어")
    await coordinator.drain()
    assert ("release", "run_7", "됐어") in pool_spy.calls and ("wait", "run_7") in pool_spy.calls


async def test_a_released_run_holds_a_work_slot_until_it_ends(
    coordinator, fake_brain, pool_spy, pid, projects
):
    pool_spy.gate = asyncio.Event()  # the agenda run below never ends on its own
    ready_item(projects, pid, title="A사 가격표")
    await coordinator.tick()
    assert len(pool_spy.runs) == 1 and not coordinator.dispatch.work_full()
    pool_spy.add_active("run_7", pid, status="waiting_human")  # taken over by the human
    finish = asyncio.Event()
    waited = pool_spy.wait

    async def wait(run_id):
        await finish.wait()
        return await waited(run_id)

    pool_spy.wait = wait
    fake_brain.returns(mc(category="release"))
    r = await coordinator.handle_tell(CH, "됐어")
    assert "이어서" in r["ack"]
    assert coordinator.dispatch.work_full()  # agenda run + released run = the 2-slot cap
    ready_item(projects, pid, title="B사 가격표")
    await coordinator.tick()
    assert len(pool_spy.runs) == 1  # no third work run while the released one is live
    finish.set()
    for _ in range(5):
        await asyncio.sleep(0)
    assert not coordinator.dispatch.work_full()
    await coordinator.tick()
    assert len(pool_spy.runs) == 2
    pool_spy.gate.set()
    await coordinator.drain()


async def test_steer_with_two_runs_and_no_hint_asks_which(coordinator, fake_brain, pool_spy, pid):
    pool_spy.add_active("run_7", pid)
    pool_spy.add_active("run_8", pid)
    fake_brain.returns(mc(category="steer", target_run_hint=None))
    r = await coordinator.handle_tell(CH, "요금제만 봐")
    assert not [c for c in pool_spy.calls if c[0] == "steer"]
    assert "run_7" in r["ack"] and "run_8" in r["ack"]


# --- discover / improve -----------------------------------------------------------------


def dispatched(log, pid, *, n, task=None, ts=NOW - timedelta(hours=1)):
    for _ in range(n):
        log.append(
            Event(
                id="",
                ts=ts,
                kind="agenda.dispatched",
                actor="coordinator",
                project_id=pid,
                run_id=None,
                payload={"kind": "improve" if task else "advance", "improve_task": task},
                refs=[],
            )
        )


async def test_discover_result_becomes_triage_backlog(stack, fake_brain, projects, tmp_path):
    gap = KR(id="kr_1", metric="추적 경쟁사 수", target=5, current=2, source="")
    pid = projects.create(make_project(krs=[gap]))
    c = stack["make"]()
    out = tmp_path / "found.md"
    out.write_text("D사가 새 요금제를 냈다 https://d.example/pricing")
    art = {"id": "art_d", "title": "발굴", "path": str(out), "status": "verified",
           "evidence": [], "verifier_run_id": "run_v"}  # fmt: skip
    stack["pool"].result = lambda order, rid: result(rid, summary="후보 2개", artifacts=[art])
    fake_brain.returns(
        Candidates(
            items=[
                Candidate(title="D사 요금제 비교", intent="D사 가격표 정리", value=4, reason="KR"),
                Candidate(title="E사 블로그 구독", intent="E사 새 글 감시", value=2, reason="약함"),
            ]
        ),
        purpose="value_estimate",
    )
    await c.tick()
    await c.drain()
    [run] = stack["pool"].runs
    assert run["order"]["kind"] == "discover" and run["order"]["project_id"] == pid
    [call] = [x for x in fake_brain.calls if x["purpose"] == "value_estimate"]
    assert call["schema"] is Candidates and "D사가 새 요금제를 냈다" in call["prompt"]
    items = projects.get(pid)["backlog"]
    assert [(b["title"], b["status"], b["value"]) for b in items] == [
        ("D사 요금제 비교", "triage", 4),
        ("E사 블로그 구독", "triage", 2),
    ]
    await c.aclose()


async def test_replay_failed_reruns_the_order_with_same_variant_and_failure(
    coordinator, projects, pid, pool_spy, log
):
    bid = ready_item(projects, pid)

    def first_fails(order, rid):
        if rid == "run_1":
            return result(
                rid, status="failed", failure={"what": "가격표", "why": "로그인 벽", "run_id": rid}
            )
        return result(rid)

    pool_spy.result = first_fails
    await coordinator.tick()  # advance fails; nothing ready -> improve: replay_failed
    await coordinator.drain()
    await coordinator.tick()
    await coordinator.drain()
    first, replay = pool_spy.runs
    o = replay["order"]
    assert o["id"] != first["order"]["id"] and o["backlog_item_id"] == bid
    assert o["kind"] == "advance" and "로그인 벽" in o["intent"] and replay["variant"] == V
    assert replay["lease"]["purpose"] == "improve"
    d = [e["payload"] for e in kinds(log, "agenda.dispatched")]
    assert d[-1]["improve_task"] == "replay_failed" and d[-1]["replay_of"] == "run_1"
    assert projects.get(pid)["backlog"][0]["status"] == "done"
    await coordinator.tick()  # replayed once only
    await coordinator.drain()
    assert len(pool_spy.runs) == 2


async def test_memory_consolidate_merges_learnings_and_rotates(
    coordinator, projects, pid, fake_brain, log, stack
):
    from swarm.coordinator.improve import Consolidation

    for t in ("C사는 화요일에 가격을 바꾼다", "C사 가격 변경은 화요일"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(Consolidation(text="C사는 화요일에 가격을 바꾼다 (2회 관찰)"), "report")
    await coordinator.tick()  # replay_failed has nothing -> memory_consolidate
    await coordinator.drain()
    [call] = [x for x in fake_brain.calls if x["purpose"] == "report"]
    assert "C사 가격 변경은 화요일" in call["prompt"]
    [e] = kinds(log, "project.global_memory_set")
    assert e["payload"] == {
        "key": "learnings:compet",
        "text": "C사는 화요일에 가격을 바꾼다 (2회 관찰)",
    }
    row = (
        stack["db"]
        .execute("SELECT value FROM coordinator_kv WHERE key='improve_rotation'")
        .fetchone()
    )
    assert row["value"] == '["replay_failed", "memory_consolidate"]'
    await coordinator.tick()  # nothing new to consolidate
    await coordinator.drain()
    assert len([x for x in fake_brain.calls if x["purpose"] == "report"]) == 1


@pytest.mark.parametrize(("others", "runs"), [(2, False), (9, True)])
async def test_meta_cap_drops_memory_consolidate(
    coordinator, projects, pid, fake_brain, log, others, runs
):
    from swarm.coordinator.improve import Consolidation

    dispatched(log, pid, n=1, task="memory_consolidate")
    dispatched(log, pid, n=others)  # 1/3 >= 15% -> capped; 1/10 < 15% -> allowed
    for t in ("a", "b"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(Consolidation(text="ab"), "report")
    await coordinator.tick()
    await coordinator.drain()
    assert bool([x for x in fake_brain.calls if x["purpose"] == "report"]) is runs


# --- kanban / dropped lines ---------------------------------------------------------------


def human_event(kind, **payload) -> Event:
    return Event(id="", ts=NOW, kind=kind, actor="human", project_id=None, run_id=None,
                 payload=payload, refs=[])  # fmt: skip


async def test_kanban_reorder_makes_the_item_next(coordinator, projects, pid, pool_spy):
    ready_item(projects, pid, title="큰 일", value=5)
    small = ready_item(projects, pid, title="작은 일", value=3)
    await coordinator.submit(human_event("human.kanban_reorder", card_key=small, priority=10))
    await coordinator.tick()
    await coordinator.drain()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][1]["priority"] == 10
    await coordinator.tick()
    await coordinator.drain()
    assert pool_spy.runs[0]["order"]["backlog_item_id"] == small


async def test_kanban_archive_drops_item_and_cancels_its_run(coordinator, projects, pid, pool_spy):
    bid = ready_item(projects, pid)
    pool_spy.gate = asyncio.Event()
    await coordinator.tick()
    await asyncio.sleep(0)
    await coordinator.submit(human_event("human.kanban_archive", card_key=bid))
    await coordinator.tick()
    for _ in range(5):
        await asyncio.sleep(0)
    assert projects.get(pid)["backlog"][0]["status"] == "dropped"
    assert ("cancel", "run_1", "사람이 칸반에서 버림") in pool_spy.calls
    pool_spy.gate.set()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "dropped"  # never resurrected


async def test_kanban_comment_is_handled_like_a_tell(
    coordinator, projects, pid, fake_brain, notifier
):
    bid = ready_item(projects, pid, title="C사 가격표")
    fake_brain.returns(mc(category="feedback", summary="표에 날짜도"))
    await coordinator.submit(
        human_event("human.kanban_comment", card_key=bid, comment="날짜 넣어줘")
    )
    await coordinator.tick()
    await coordinator.drain()
    assert (
        "날짜 넣어줘" in fake_brain.calls[0]["prompt"]
        and "C사 가격표" in fake_brain.calls[0]["prompt"]
    )
    assert projects.get(pid)["memory"]["learnings"]
    [m] = notifier.sent
    assert m["channel"] == CH and m["kind"] == "ack"


async def test_kanban_comment_reply_is_urgent(coordinator, projects, pid, fake_brain, notifier):
    """The comment is the human's own message, so quiet hours never hold the reply."""
    bid = ready_item(projects, pid, title="C사 가격표")
    fake_brain.returns(mc(category="feedback", summary="표에 날짜도"))
    await coordinator.submit(
        human_event("human.kanban_comment", card_key=bid, comment="날짜 넣어줘")
    )
    await coordinator.tick()
    await coordinator.drain()
    [m] = notifier.sent
    assert m["kind"] == "ack" and m["urgent"]


async def test_pending_dropped_lines_are_surfaced_once(coordinator, log, pid, notifier):
    log.append(
        Event(id="", ts=NOW, kind="run.pending_dropped", actor="workers", project_id=pid,
              run_id="run_3", payload={"lines": ["사람 지시: 요금제만 봐"]}, refs=[])
    )  # fmt: skip
    await coordinator.tick()
    await coordinator.tick()
    await coordinator.drain()
    [m] = notifier.sent
    assert m["channel"] == CH and m["kind"] == "fyi" and "요금제만 봐" in m["text"]


# --- reports ------------------------------------------------------------------------------


def reports(notifier, title):
    return [m for m in notifier.sent if m["kind"] == "report" and title in m["text"]]


async def test_daily_report_at_8_is_sent_once(stack, pid, clock, notifier):
    clock.advance(timedelta(hours=4, minutes=59))  # 07:59
    c = stack["make"]()
    await c.tick()
    await c.drain()
    assert reports(notifier, "일일 보고") == []
    clock.advance(timedelta(minutes=2))  # 08:01
    await c.tick()
    await c.tick()
    await c.drain()
    [m] = reports(notifier, "일일 보고")
    assert m["channel"] == GENERAL and m["text"].startswith("📊 일일 보고")
    await c.aclose()
    c2 = stack["make"]()  # restart the same day: the sent date is persisted
    await c2.tick()
    await c2.drain()
    assert len(reports(notifier, "일일 보고")) == 1
    await c2.aclose()


async def test_weekly_report_friday_17_goes_to_each_project(stack, pid, clock, notifier):
    clock.advance(timedelta(days=1, hours=14, minutes=5))  # Fri 2026-11-06 17:05
    assert clock.now().weekday() == 4
    c = stack["make"]()
    await c.tick()
    await c.tick()
    await c.drain()
    [m] = reports(notifier, "주간 보고")
    assert m["channel"] == CH and m["text"].startswith("📊 주간 보고")
    await c.aclose()


async def test_weekly_report_waits_for_17_on_friday(stack, pid, clock, notifier):
    clock.advance(timedelta(days=1, hours=13, minutes=59))  # Fri 2026-11-06 16:59
    assert clock.now().weekday() == 4
    c = stack["make"]()
    await c.tick()
    await c.drain()
    assert reports(notifier, "주간 보고") == []
    clock.advance(timedelta(minutes=2))  # 17:01
    await c.tick()
    await c.drain()
    assert len(reports(notifier, "주간 보고")) == 1
    await c.aclose()


async def test_report_still_rendering_is_not_started_again(stack, pid, clock, notifier, fake_brain):
    clock.advance(timedelta(hours=5, minutes=1))  # 08:01
    rendered = asyncio.Event()
    decide = fake_brain.decide

    async def slow_report(**kw):
        if kw["purpose"] == "report":
            await rendered.wait()
        return await decide(**kw)

    fake_brain.decide = slow_report
    c = stack["make"]()
    await c.tick()
    await asyncio.sleep(0)
    await c.tick()  # the daily report is still rendering
    rendered.set()
    await c.drain()
    assert len([x for x in fake_brain.calls if x["purpose"] == "report"]) == 1
    assert len(reports(notifier, "일일 보고")) == 1
    await c.aclose()


async def test_report_on_demand_uses_the_channel(coordinator, notifier):
    assert (await coordinator.report(CH, "weekly")).startswith("📊 주간 보고")
    assert (await coordinator.report(GENERAL, "daily")).startswith("📊 일일 보고")
    assert notifier.sent == []  # returned to the caller, not pushed


# --- cleanup, resume backoff and restart recovery -----------------------------------------


async def test_cancelled_run_does_not_leave_its_item_running(coordinator, projects, pid, pool_spy):
    ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(rid, status="cancelled", summary="cancelled: stuck")
    await coordinator.tick()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "blocked"


async def test_task_cancelled_before_its_first_step_still_cleans_up(
    coordinator, projects, pid, pool_spy, stack
):
    pool_spy.gate = asyncio.Event()  # never released
    ready_item(projects, pid)
    await coordinator.tick()
    await coordinator.aclose()  # no yield in between: the run task never got a turn
    ledger = stack["ledger"]
    assert all(ledger.lease(engine_pref=["claude"], purpose="advance") for _ in range(3))
    assert coordinator.dispatch.running_count() == 0


async def parked_with_answer(coordinator, projects, pid, pool_spy, inbox):
    """A coordinator-dispatched run parked on an Ask that the human has answered."""
    ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(rid, status="waiting_human")
    await coordinator.tick()
    await coordinator.drain()
    order_id = pool_spy.runs[0]["order"]["id"]
    pool_spy.add_active("run_1", pid, status="waiting_human", order_id=order_id)
    t = await inbox.raise_ask("run_1", draft())
    await inbox.answer(t["ask_id"], choice="B", raw_text="B", via=CH)
    return t["ask_id"]


def wake_event() -> Event:
    return Event(id="", ts=NOW, kind="coordinator.wake", actor="test", project_id=None,
                 run_id=None, payload={}, refs=[])  # fmt: skip


async def test_resume_retries_back_off_then_give_up_tells_the_human(
    coordinator, projects, pid, pool_spy, inbox, clock, log, notifier
):
    await parked_with_answer(coordinator, projects, pid, pool_spy, inbox)
    pool_spy.resume_errors = [IllegalTransition("changed while resuming")] * 10
    stop = asyncio.Event()
    task = asyncio.create_task(coordinator.agenda_loop(stop))
    await asyncio.sleep(0.1)
    assert len(pool_spy.resumes) == 1  # the failed task's wake must not burn the retries
    for n, delay in enumerate((60, 300, 900, 3600), start=2):
        clock.advance(timedelta(seconds=delay - 5))
        await coordinator.submit(wake_event())
        await asyncio.sleep(0.05)
        assert len(pool_spy.resumes) == n - 1  # not due yet
        clock.advance(timedelta(seconds=10))
        await coordinator.submit(wake_event())
        await asyncio.sleep(0.05)
        assert len(pool_spy.resumes) == n
    [m] = [m for m in notifier.sent if m["kind"] == "fyi"]
    assert m["channel"] == CH and "#1" in m["text"] and "B" in m["text"]
    notes = [e["payload"]["note"] for e in kinds(log, "project.backlog_updated")]
    assert "답 전달 실패" in notes[-1]
    assert kinds(log, "coordinator.resume_failed")
    stop.set()
    await asyncio.wait_for(task, 1)


async def test_given_up_resume_marks_its_order_error(coordinator, projects, pid, pool_spy, inbox):
    """The run that order was held for will not continue: the order is closed as an error."""
    await parked_with_answer(coordinator, projects, pid, pool_spy, inbox)
    order_id = pool_spy.runs[0]["order"]["id"]
    pool_spy.resume_errors = [KeyError("run_1")]  # the run is gone: give up at once
    await coordinator.tick()
    await coordinator.drain()
    assert len(pool_spy.resumes) == 1
    assert coordinator.records.order_row(order_id)["state"] == "error"


async def test_resume_lease_covers_the_order_budget(
    coordinator, projects, pid, pool_spy, inbox, stack, clock
):
    await parked_with_answer(coordinator, projects, pid, pool_spy, inbox)
    stack["db"].execute(
        "UPDATE coordinator_orders SET order_json=json_set(order_json, '$.budget.max_minutes', 120)"
    )
    await coordinator.tick()
    await coordinator.drain()
    [r] = pool_spy.resumes
    assert r["lease"]["expires_at"] == clock.now() + timedelta(minutes=130)


async def test_leftover_confirmation_row_is_applied_after_restart(
    stack, coordinator, fake_brain, inbox, pid, pool_spy, log, monkeypatch, clock
):
    fake_brain.returns(mc(category="goal_change", summary="8곳으로"))
    await coordinator.handle_tell(CH, "경쟁사 8곳으로 늘리자")
    [ask] = inbox.open_asks(pid)
    await inbox.answer(ask["ask_id"], choice="바꿔요", raw_text="바꿔요", via=CH)

    async def broken(o):
        raise RuntimeError("notifier down")

    monkeypatch.setattr(coordinator.resumes, "apply_confirmation", broken)
    await coordinator.tick()  # drained and persisted, but applying it failed
    await coordinator.drain()
    assert kinds(log, "coordinator.change_confirmed") == []
    await coordinator.aclose()
    clock.advance(timedelta(seconds=61))  # past the first retry backoff
    c2 = stack["make"]()
    await c2.tick()
    await c2.drain()
    assert pool_spy.resumes == []  # never pool.resume("coord_...")
    [e] = kinds(log, "coordinator.change_confirmed")
    assert e["payload"]["summary"] == "8곳으로"
    await c2.tick()
    await c2.drain()
    assert len(kinds(log, "coordinator.change_confirmed")) == 1
    await c2.aclose()


async def test_leftover_expired_row_is_cleared(coordinator, stack, pool_spy, log):
    o = AskOutcome(ask_id="ask_old", run_id="run_gone", status="expired", choice=None,
                   raw_text=None, via="expired")  # fmt: skip
    stack["db"].execute(
        "INSERT INTO coordinator_resumes(ask_id, outcome_json, state, created_at)"
        " VALUES (?,?,'pending',?)",
        ("ask_old", json.dumps(o), NOW.isoformat()),
    )
    await coordinator.tick()
    await coordinator.drain()
    assert pool_spy.resumes == []
    assert [e["payload"]["ask_id"] for e in kinds(log, "coordinator.ask_expired")] == ["ask_old"]
    assert stack["db"].execute("SELECT count(*) FROM coordinator_resumes").fetchone()[0] == 0


async def test_background_tell_failure_tells_the_human(stack, pid, fake_brain, notifier):
    c = stack["make"](ack_timeout_s=0.05)
    fake_brain.returns(RuntimeError("bug"))
    fake_brain.delay = 0.2
    r = await c.handle_tell(CH, "C사 봐줘")
    assert r["ack"] == INTERIM_ACK
    await c.drain()
    [m] = notifier.sent
    assert m["kind"] == "fyi" and m["channel"] == CH and "다시 보내 주세요" in m["text"]
    await c.aclose()


async def test_tell_failure_inside_the_ack_window_returns_a_failure_ack(coordinator, fake_brain):
    fake_brain.returns(RuntimeError("bug"))
    r = await coordinator.handle_tell(CH, "C사 봐줘")
    assert "다시 보내 주세요" in r["ack"] and r["order_id"] is None


async def test_low_confidence_answer_reasks_instead_of_closing(
    coordinator, inbox, fake_brain, pool_spy, pid
):
    pool_spy.add_active("run_1", pid, status="waiting_human")
    await inbox.raise_ask("run_1", draft())
    fake_brain.returns(mc(category="ask_answer", target_ask_number=1, confidence=0.3))
    r = await coordinator.handle_tell(CH, "#1 B")
    assert r["closed_ask_id"] is None and "#1" in r["ack"]
    assert len(inbox.open_asks(pid)) == 1


async def test_blank_summary_falls_back_to_the_text(coordinator, fake_brain, projects, pid):
    fake_brain.returns(mc(summary="   "))
    r = await coordinator.handle_tell(CH, "C사 봐줘")
    assert r["order_id"]
    assert projects.get(pid)["backlog"][0]["title"] == "C사 봐줘"


async def test_negated_answer_goes_to_the_brain_not_the_named_option(
    coordinator, inbox, fake_brain, pool_spy, pid
):
    from swarm.coordinator.messages import AnswerChoice

    pool_spy.add_active("run_1", pid, status="waiting_human")
    await inbox.raise_ask("run_1", draft())
    fake_brain.returns(mc(category="ask_answer", target_ask_number=1))
    fake_brain.returns(AnswerChoice(choice="A", confidence=0.9), purpose="map_answer")
    r = await coordinator.handle_tell(CH, "#1 B는 별로")
    assert "A로 진행" in r["ack"]
    assert [c["purpose"] for c in fake_brain.calls] == ["classify_message", "map_answer"]


async def test_consolidate_is_not_dispatched_twice_while_in_flight(
    coordinator, projects, pid, fake_brain, log
):
    from swarm.coordinator.improve import Consolidation

    dispatched(log, pid, n=30)  # keep the meta share under 15%
    for t in ("a", "b"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(Consolidation(text="ab"), "report")
    fake_brain.delay = 0.05
    await coordinator.tick()
    await coordinator.tick()  # the first Brain call is still running
    await coordinator.drain()
    assert len([c for c in fake_brain.calls if c["purpose"] == "report"]) == 1


async def test_consolidate_failure_is_retried_later(
    coordinator, projects, pid, fake_brain, log, clock
):
    from swarm.coordinator.improve import Consolidation

    dispatched(log, pid, n=30)  # keep the meta share under 15%
    for t in ("a", "b"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(BrainError("engine unavailable"), "report")
    await coordinator.tick()
    await coordinator.drain()
    fake_brain.returns(Consolidation(text="ab"), "report")
    clock.advance(timedelta(minutes=6))  # after the retry wait
    await coordinator.tick()
    await coordinator.drain()
    assert len([c for c in fake_brain.calls if c["purpose"] == "report"]) == 2


async def test_report_date_is_stored_only_after_a_successful_send(stack, pid, clock, notifier):
    clock.advance(timedelta(hours=5, minutes=1))  # 08:01
    fail = {"n": 1}
    real_send = notifier.send

    async def flaky(msg):
        if fail["n"]:
            fail["n"] -= 1
            raise RuntimeError("outbox down")
        await real_send(msg)

    notifier.send = flaky
    c = stack["make"]()
    await c.tick()
    await c.drain()
    assert reports(notifier, "일일 보고") == []
    clock.advance(timedelta(minutes=5, seconds=1))  # past the report backoff
    await c.tick()  # retried, then deduped
    await c.drain()
    await c.tick()
    await c.drain()
    assert len(reports(notifier, "일일 보고")) == 1
    await c.aclose()


# --- report backoff, consolidation, held tells across a restart ---------------------------


async def test_failing_report_send_renders_once_per_backoff_window(
    stack, pid, clock, notifier, fake_brain
):
    clock.advance(timedelta(hours=5, minutes=1))  # 08:01

    async def down(msg):
        raise RuntimeError("outbox down")

    notifier.send = down
    c = stack["make"]()

    def renders():
        return len([x for x in fake_brain.calls if x["purpose"] == "report"])

    for _ in range(5):  # each failed task wakes the loop: no re-render inside the window
        await c.tick()
        await c.drain()
    assert renders() == 1
    for n, window in enumerate((300, 900, 3600, 3600), start=2):
        clock.advance(timedelta(seconds=window - 10))
        await c.tick()
        await c.drain()
        assert renders() == n - 1
        clock.advance(timedelta(seconds=20))
        await c.tick()
        await c.drain()
        assert renders() == n
    await c.aclose()


async def test_consolidate_mark_is_restored_on_any_error(
    coordinator, projects, pid, fake_brain, log, stack, monkeypatch
):
    from swarm.coordinator.improve import Consolidation

    dispatched(log, pid, n=30)
    for t in ("a", "b"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(Consolidation(text="ab"), "report")

    def broken(entry):
        raise RuntimeError("disk")

    monkeypatch.setattr(projects, "add_global_memory", broken)
    await coordinator.tick()
    await coordinator.drain()
    row = (
        stack["db"].execute("SELECT value FROM coordinator_kv WHERE key='consolidated'").fetchone()
    )
    assert json.loads(row["value"]) == {pid: 0}
    assert kinds(log, "coordinator.consolidate_failed")


async def test_confirmation_that_keeps_failing_backs_off_then_gives_up(
    coordinator, fake_brain, inbox, pid, clock, notifier, stack, monkeypatch
):
    fake_brain.returns(mc(category="goal_change", summary="8곳으로"))
    await coordinator.handle_tell(CH, "경쟁사 8곳으로 늘리자")
    [ask] = inbox.open_asks(pid)
    await inbox.answer(ask["ask_id"], choice="바꿔요", raw_text="바꿔요", via=CH)
    notifier.sent.clear()
    calls = []

    async def broken(o):
        calls.append(o["ask_id"])
        raise RuntimeError("store down")

    monkeypatch.setattr(coordinator.resumes, "apply_confirmation", broken)
    await coordinator.tick()
    await coordinator.tick()  # not due again yet
    assert len(calls) == 1
    for delay in (60, 300, 900, 3600):
        clock.advance(timedelta(seconds=delay + 1))
        await coordinator.tick()
    assert len(calls) == 5
    [m] = notifier.sent
    assert (
        m["channel"] == CH and m["kind"] == "fyi" and "확인한 변경을 적용하지 못했어요" in m["text"]
    )
    assert stack["db"].execute("SELECT count(*) FROM coordinator_resumes").fetchone()[0] == 0
    clock.advance(timedelta(hours=2))
    await coordinator.tick()
    assert len(calls) == 5


# --- held human events survive a restart ------------------------------------------------


def held_rows(stack) -> int:
    return stack["db"].execute("SELECT count(*) FROM coordinator_held").fetchone()[0]


async def test_held_tell_survives_restart_and_the_reply_is_sent(
    stack, pid, fake_brain, notifier, clock
):
    c1 = stack["make"]()
    fake_brain.returns(BrainError("engine unavailable"))
    r = await c1.handle_tell(CH, "C사 봐줘")
    assert r["ack"] == INTERIM_ACK
    await c1.drain()
    assert held_rows(stack) == 1  # the promised reply is on disk, not only in memory
    await c1.aclose()
    fake_brain.returns(mc())
    clock.advance(timedelta(seconds=61))
    c2 = stack["make"]()  # restart
    await c2.tick()
    await c2.drain()
    [m] = [m for m in notifier.sent if m["kind"] == "ack"]
    assert m["channel"] == CH and "받았어요" in m["text"]
    assert held_rows(stack) == 0  # handled -> deleted
    c3 = stack["make"]()  # nothing is replayed twice
    await c3.tick()
    await c3.drain()
    assert len([m for m in notifier.sent if m["kind"] == "ack"]) == 1
    await c2.aclose()
    await c3.aclose()


async def test_tell_in_flight_at_shutdown_is_retried_after_restart(
    stack, pid, fake_brain, notifier, projects
):
    c1 = stack["make"](ack_timeout_s=0.01)
    fake_brain.returns(mc())
    fake_brain.delay = 5.0  # still classifying when the daemon stops
    r = await c1.handle_tell(CH, "C사 무료 티어 영향")
    assert r["ack"] == INTERIM_ACK
    await c1.aclose()  # shutdown cancels the tell mid-flight
    assert held_rows(stack) == 1
    fake_brain.delay = 0.0
    c2 = stack["make"]()
    await c2.tick()
    await c2.drain()
    assert [m["kind"] for m in notifier.sent if m["kind"] in ("ack", "fyi")] == ["ack"]
    assert len(projects.get(pid)["backlog"]) == 1
    assert held_rows(stack) == 0
    await c2.aclose()


async def test_tell_that_fails_is_not_replayed_after_restart(stack, pid, fake_brain, notifier):
    c1 = stack["make"]()
    fake_brain.returns(RuntimeError("bug"))
    r = await c1.handle_tell(CH, "C사 봐줘")
    assert r["ack"].startswith(FAILED_ACK)  # the human already heard it failed
    await c1.drain()
    assert held_rows(stack) == 0
    await c1.aclose()
    c2 = stack["make"]()
    await c2.tick()
    await c2.drain()
    assert notifier.sent == []
    await c2.aclose()


async def test_submitted_kanban_event_survives_restart(stack, pid, projects, pool_spy):
    c1 = stack["make"]()
    small = ready_item(projects, pid, title="작은 일", value=3)
    await c1.submit(human_event("human.kanban_reorder", card_key=small, priority=10))
    assert held_rows(stack) == 1
    await c1.aclose()  # died before its respond turn
    c2 = stack["make"]()
    await c2.tick()
    await c2.drain()
    assert projects.get(pid)["backlog"][0]["priority"] == 10
    assert held_rows(stack) == 0
    await c2.aclose()


async def test_on_tick_hook_runs_after_each_completed_tick_only(stack, pid, monkeypatch):
    ticks: list[int] = []
    c = stack["make"](on_tick=lambda: ticks.append(1), tick_every_s=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(c.agenda_loop(stop))
    await asyncio.sleep(0.1)
    assert len(ticks) >= 2  # the daemon touches ~/swarm/heartbeat here
    seen = len(ticks)

    async def hung():
        await asyncio.Event().wait()

    monkeypatch.setattr(c, "tick", hung)  # a hung tick never reports progress
    await asyncio.sleep(0.1)
    assert len(ticks) <= seen + 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await c.aclose()


class TickingClock(FakeClock):
    """Like the system clock: every read is a little later than the previous one."""

    def now(self):
        self.advance(timedelta(milliseconds=1))
        return super().now()


async def test_engine_is_available_with_a_clock_that_moves(tmp_path, fake_brain, notifier):
    """Regression (found assembling swarmd): ledger.next_reset() returns *its* now when the
    engine is not cooling, which is later than the tick's `now` on a real clock — the agenda
    must not read that as "cooling", and status must not claim the engine is resting."""
    clock = TickingClock(NOW)
    db = Database(tmp_path / "t.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    pool = FakePool(ledger, clock)
    inbox = SqliteInbox(db, log, projects, fake_brain, clock, run_project=pool.run_project,
                        general_channel=GENERAL)  # fmt: skip
    c = Coordinator(db=db, log=log, projects=projects, inbox=inbox, ledger=ledger, pool=pool,
                    brain=fake_brain, notifier=notifier, clock=clock, general_channel=GENERAL,
                    default_profile="work", default_variant=V)  # fmt: skip
    pid = projects.create(make_project())
    ready_item(projects, pid)
    await c.tick()
    await c.drain()
    assert len(pool.runs) == 1
    assert "쉬는 중" not in await c.status(CH)
    await c.aclose()


def test_daily_report_facts_see_no_cooling_with_a_clock_that_moves(tmp_path):
    from swarm.coordinator.reports import build_daily

    clock = TickingClock(NOW)
    db = Database(tmp_path / "r.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    inbox = SqliteInbox(db, log, projects, ScriptBrain(), clock, run_project=lambda r: None,
                        general_channel=GENERAL)  # fmt: skip
    assert build_daily(log, projects, inbox, ledger, clock.now()).cooling_until is None
    ledger.observe(
        engine="claude",
        signal=UsageSignal(kind="limit_hit", reset_at=clock.now() + timedelta(hours=1),
                           at=clock.now()),
    )  # fmt: skip
    assert build_daily(log, projects, inbox, ledger, clock.now()).cooling_until is not None


# --- item interrupts, urgent replies, work slots and retry schedules ----------------------


async def run_loop_until(coordinator, done, *, real_s=3.0):
    """Drive the real agenda loop (fast ticks) until `done()` or `real_s` wall seconds."""
    stop = asyncio.Event()
    task = asyncio.create_task(coordinator.agenda_loop(stop))
    try:
        for _ in range(int(real_s / 0.01)):
            if done():
                break
            await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)
        await coordinator.drain()


async def test_interrupted_runs_back_off_then_block_the_item_with_one_fyi(
    stack, pid, projects, pool_spy, notifier, clock, log
):
    """A pool that always ends `interrupted` (Chrome cannot start) must not be redispatched
    hot: the item waits 60 s, then 5 min, and the 3rd interrupt in a row blocks it + one FYI."""
    bid = ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(
        rid, status="interrupted", summary="browser unavailable: chrome exited early"
    )
    c = stack["make"](tick_every_s=0.001, on_tick=lambda: clock.advance(timedelta(seconds=30)))
    t0 = clock.now()

    def blocked():
        return projects.get(pid)["backlog"][0]["status"] == "blocked"

    await run_loop_until(c, lambda: blocked() and clock.now() - t0 > timedelta(hours=1))
    assert blocked()
    assert len(pool_spy.runs) == 3  # bounded: not one dispatch per tick
    ts = [e["ts"] for e in kinds(log, "agenda.dispatched")]
    assert ts[1] - ts[0] >= timedelta(seconds=60) and ts[2] - ts[1] >= timedelta(seconds=300)
    fyis = [m for m in notifier.sent if m["kind"] == "fyi"]
    assert len(fyis) == 1 and "3번 연속 중단" in fyis[0]["text"] and fyis[0]["channel"] == CH
    [stopped] = kinds(log, "coordinator.item_stopped")
    assert stopped["payload"]["item_id"] == bid


async def test_done_resets_the_interrupt_count(stack, pid, projects, pool_spy, clock, log):
    ready_item(projects, pid)
    outcomes = ["interrupted", "interrupted", "done"]
    pool_spy.result = lambda order, rid: result(
        rid, status=outcomes.pop(0) if outcomes else "done", summary="verifier unavailable: x"
    )
    c = stack["make"](tick_every_s=0.001, on_tick=lambda: clock.advance(timedelta(seconds=30)))
    await run_loop_until(c, lambda: projects.get(pid)["backlog"][0]["status"] == "done")
    assert projects.get(pid)["backlog"][0]["status"] == "done"
    assert stack["db"].execute("SELECT count(*) FROM coordinator_item_backoff").fetchone()[0] == 0


async def test_quota_pause_is_not_counted_as_an_interrupt(
    coordinator, projects, pid, pool_spy, stack
):
    ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(
        rid, status="interrupted", summary="paused: quota:limit_hit"
    )
    await coordinator.tick()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "ready"
    db = stack["db"]
    assert db.execute("SELECT count(*) FROM coordinator_item_backoff").fetchone()[0] == 0


async def test_reply_to_a_tell_at_2330_is_not_held_by_quiet_hours(stack, pid, fake_brain, clock):
    """The human just wrote, so the late reply goes out at once, not at 08:00."""
    import httpx

    from swarm.clock import AlertWindow
    from swarm.front.notifier import TelegramNotifier

    sent: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content)["text"])
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent)}})

    clock.advance(timedelta(hours=20, minutes=30))  # Thursday 23:30, quiet hours
    tg = TelegramNotifier(
        "1:tok", stack["db"], stack["inbox"], clock, AlertWindow(time(8, 0), time(23, 0)),
        transport=httpx.MockTransport(handle),
    )  # fmt: skip
    c = stack["make"](ack_timeout_s=0.01, notifier=tg)
    fake_brain.returns(mc(category="policy_add", summary="가격은 원화로"))
    fake_brain.delay = 0.05
    assert (await c.handle_tell(CH, "가격은 원화로 적어"))["ack"] == INTERIM_ACK
    await c.drain()
    await tg.drain_once()
    assert sent == ["정책 +1: 가격은 원화로"]
    await c.aclose()


async def test_every_reply_to_a_human_message_is_urgent(coordinator, fake_brain, notifier, clock):
    """The held-tell retry reply and the final "처리하지 못했어요" are urgent too."""
    fake_brain.returns(BrainError("engine unavailable"))
    await coordinator.handle_tell(CH, "C사 봐줘")
    fake_brain.returns(mc())
    clock.advance(timedelta(seconds=61))
    await coordinator.tick()
    await coordinator.drain()
    fake_brain.returns(BrainError("engine unavailable"))
    await coordinator.handle_tell(CH, "D사 봐줘")
    for delay in (61, 301, 901):
        clock.advance(timedelta(seconds=delay))
        await coordinator.tick()
        await coordinator.drain()
    replies = [m for m in notifier.sent if m["kind"] in ("ack", "fyi")]
    assert {m["text"][:5] for m in replies} == {"받았어요.", FAILED_ACK_HEAD}
    assert all(m["urgent"] for m in replies)


async def test_replies_to_the_human_are_always_urgent(stack, pid, fake_brain, notifier, clock):
    """The reply after an interim ack, FAILED_ACK after a background failure, the reply to a
    held tell and FAILED_ACK when a held tell runs out of retries: all urgent."""
    c = stack["make"](ack_timeout_s=0.01)
    fake_brain.delay = 0.05  # slower than the ack window: the human gets the interim ack
    fake_brain.returns(mc(category="policy_add", summary="가격은 원화로"))
    assert (await c.handle_tell(CH, "가격은 원화로 적어"))["ack"] == INTERIM_ACK
    await c.drain()
    fake_brain.returns(RuntimeError("bug"))
    assert (await c.handle_tell(CH, "C사 봐줘"))["ack"] == INTERIM_ACK
    await c.drain()
    fake_brain.delay = 0.0
    fake_brain.returns(BrainError("engine unavailable"))
    await c.handle_tell(CH, "통화는 원화")
    fake_brain.returns(mc(category="policy_add", summary="통화는 원화"))
    clock.advance(timedelta(seconds=61))
    await c.tick()
    await c.drain()
    fake_brain.returns(BrainError("engine unavailable"))
    await c.handle_tell(CH, "D사 봐줘")
    for delay in (61, 301, 901):
        clock.advance(timedelta(seconds=delay))
        await c.tick()
        await c.drain()
    assert [(m["kind"], m["text"]) for m in notifier.sent] == [
        ("ack", "정책 +1: 가격은 원화로"),
        ("fyi", FAILED_ACK),
        ("ack", "정책 +1: 통화는 원화"),
        ("fyi", f"{FAILED_ACK}: D사 봐줘"),
    ]
    assert all(m["urgent"] for m in notifier.sent)
    await c.aclose()


async def test_held_tell_that_fails_on_retry_tells_the_human_urgently(
    coordinator, fake_brain, notifier, clock
):
    fake_brain.returns(BrainError("engine unavailable"))
    await coordinator.handle_tell(CH, "C사 봐줘")
    fake_brain.returns(RuntimeError("bug"))  # not a Brain outage: the retry itself breaks
    clock.advance(timedelta(seconds=61))
    await coordinator.tick()
    await coordinator.drain()
    [m] = notifier.sent
    assert (m["kind"], m["text"]) == ("fyi", f"{FAILED_ACK}: C사 봐줘") and m["urgent"]


def quota_stop_with_an_ask(inbox, pool_spy, asks: list[str]):
    """The run raises an Ask, then the engine hits its usage limit before the turn's Done."""

    async def during(order, run_id):
        asks.append((await inbox.raise_ask(run_id, draft()))["ask_id"])

    pool_spy.during = during
    pool_spy.result = lambda order, rid: result(
        rid, status="interrupted", summary="paused: quota:limit_hit"
    )


async def test_interrupted_run_with_an_open_ask_keeps_its_item_and_is_resumed(
    coordinator, projects, pid, pool_spy, inbox, clock, log
):
    """One item, one live run. The interrupted run still owns an open Ask, so the item is
    never re-dispatched; the human's answer resumes that same run."""
    bid = ready_item(projects, pid)
    asks: list[str] = []
    quota_stop_with_an_ask(inbox, pool_spy, asks)
    await coordinator.tick()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "blocked"  # held for its run
    for _ in range(3):  # quota cooled, hours pass: still nobody else runs the item
        clock.advance(timedelta(hours=1))
        await coordinator.tick()
        await coordinator.drain()
    assert len(pool_spy.runs) == 1
    await inbox.answer(asks[0], choice="A", raw_text="A", via=CH)
    await coordinator.tick()
    await coordinator.drain()
    assert [r["run_id"] for r in pool_spy.resumes] == ["run_1"]  # one resume, same run
    assert len(pool_spy.runs) == 1  # no re-dispatch
    assert projects.get(pid)["backlog"][0]["status"] == "done"
    [held] = kinds(log, "coordinator.item_held")
    assert held["payload"]["ask_id"] == asks[0] and held["run_id"] == "run_1"
    assert bid == projects.get(pid)["backlog"][0]["id"]


async def test_held_item_whose_ask_expires_returns_to_ready_with_backoff(
    coordinator, projects, pid, pool_spy, inbox, clock
):
    ready_item(projects, pid)
    asks: list[str] = []
    quota_stop_with_an_ask(inbox, pool_spy, asks)
    await coordinator.tick()
    await coordinator.drain()
    pool_spy.during = None
    pool_spy.result = lambda order, rid: result(rid)
    inbox.expire_for_run("run_1")
    await coordinator.tick()  # released, but on hold: not re-dispatched at once
    await coordinator.drain()
    assert len(pool_spy.runs) == 1 and pool_spy.resumes == []
    assert projects.get(pid)["backlog"][0]["status"] == "ready"
    clock.advance(timedelta(seconds=61))
    await coordinator.tick()
    await coordinator.drain()
    assert len(pool_spy.runs) == 2
    assert projects.get(pid)["backlog"][0]["status"] == "done"


async def test_run_left_with_an_open_ask_at_a_restart_keeps_its_item(
    stack, pid, projects, pool_spy, inbox
):
    """At boot: the dead instance's run (now interrupted) still has an open Ask, so the
    restarted Coordinator holds its item instead of making it ready."""
    ready_item(projects, pid)
    pool_spy.gate = asyncio.Event()
    c1 = stack["make"]()
    await c1.tick()
    await asyncio.sleep(0)
    await inbox.raise_ask("run_1", draft())
    pool_spy.ended["run_1"] = WorkerRun(**{**pool_spy.active.pop("run_1"), "status": "interrupted"})
    c2 = stack["make"]()  # the restarted daemon (the old run is no longer live)
    await c2.tick()
    await asyncio.sleep(0)
    assert projects.get(pid)["backlog"][0]["status"] == "blocked"
    assert len(pool_spy.runs) == 1
    pool_spy.gate.set()
    await c2.aclose()
    await c1.aclose()


def leasing_brain(fake_brain, ledger):
    """The real EngineBrain needs a claude lease for every decision: model that."""
    decide = fake_brain.decide

    async def leased(**kw):
        lease = ledger.lease(engine_pref=["claude"], purpose="respond", max_minutes=2)
        if lease is None:
            raise BrainError("no claude slot free")
        try:
            return await decide(**kw)
        finally:
            ledger.release(lease)

    fake_brain.decide = leased


async def test_two_busy_work_runs_leave_the_respond_slot_and_queued_work_waits(
    stack, pid, projects, pool_spy, fake_brain
):
    """With 2 work runs busy, a tell is still triaged (the 3rd claude slot is respond's),
    and the human order it queues waits for a work slot instead of taking that slot."""
    leasing_brain(fake_brain, stack["ledger"])
    pool_spy.gate = asyncio.Event()
    for i in range(2):
        ready_item(projects, pid, title=f"일 {i}")
    c = stack["make"]()
    await c.tick()
    await asyncio.sleep(0)
    assert len(pool_spy.runs) == 2
    fake_brain.returns(mc())
    r = await c.handle_tell(CH, "C사 무료 티어 영향 봐줘")
    assert r["order_id"] and r["ack"].startswith("받았어요")  # triaged at once
    await c.tick()
    await asyncio.sleep(0)
    assert len(pool_spy.runs) == 2  # the queued human order waits
    fake_brain.returns(mc(category="steer", target_run_hint="run_1"))
    r = await c.handle_tell(CH, "run_1 가격 말고 기능표 봐")
    assert r["ack"] == "다음 단계부터 반영할게요."  # a steer still gets through
    pool_spy.gate.set()
    await c.drain()
    await c.tick()
    await c.drain()
    assert pool_spy.runs[2]["order"]["id"] == r_order_id(stack, "human")  # its turn came
    await c.aclose()


def r_order_id(stack, origin):
    [row] = (
        stack["db"]
        .execute("SELECT order_id FROM coordinator_orders WHERE origin=?", (origin,))
        .fetchall()
    )
    return row["order_id"]


async def test_resume_waits_for_a_work_slot(stack, pid, projects, pool_spy, fake_brain, inbox):
    leasing_brain(fake_brain, stack["ledger"])
    pool_spy.gate = asyncio.Event()
    for i in range(2):
        ready_item(projects, pid, title=f"일 {i}")
    pool_spy.add_active("run_p", pid, status="waiting_human")
    c = stack["make"]()
    await c.tick()
    await asyncio.sleep(0)
    t = await inbox.raise_ask("run_p", draft())
    await inbox.answer(t["ask_id"], choice="A", raw_text="A", via=CH)
    await c.tick()
    await asyncio.sleep(0)
    assert pool_spy.resumes == []  # both work slots busy: the resume waits
    fake_brain.returns(mc(category="status"))
    assert "📋" in (await c.handle_tell(CH, "어떻게 돼 가?"))["ack"]  # respond still works
    pool_spy.gate.set()
    await c.drain()
    await c.tick()
    await c.drain()
    assert [x["run_id"] for x in pool_spy.resumes] == ["run_p"]
    await c.aclose()


async def test_release_waits_while_both_work_slots_are_busy(
    stack, pid, projects, pool_spy, fake_brain
):
    leasing_brain(fake_brain, stack["ledger"])
    pool_spy.gate = asyncio.Event()
    for i in range(2):
        ready_item(projects, pid, title=f"일 {i}")
    c = stack["make"]()
    await c.tick()
    await asyncio.sleep(0)
    assert len(pool_spy.runs) == 2
    pool_spy.add_active("run_7", pid, status="waiting_human")  # taken over by the human
    fake_brain.returns(mc(category="release"))
    r = await c.handle_tell(CH, "됐어")
    assert r["ack"] == "지금 엔진이 꽉 차 있어요. 잠시 후 다시 '됐어'라고 보내 주세요."
    assert [x for x in pool_spy.calls if x[0] == "release"] == []
    pool_spy.gate.set()
    await c.drain()
    await c.aclose()


async def test_a_resume_holds_a_work_slot_until_it_ends(stack, pid, projects, pool_spy, inbox):
    pool_spy.gate = asyncio.Event()  # the agenda run below never ends on its own
    ready_item(projects, pid, title="A사 가격표")
    pool_spy.add_active("run_p", pid, status="waiting_human")
    c = stack["make"]()
    await c.tick()
    assert len(pool_spy.runs) == 1 and not c.dispatch.work_full()
    finish = asyncio.Event()
    resumed = pool_spy.resume

    async def resume(run_id, *, outcome, lease):
        await finish.wait()
        return await resumed(run_id, outcome=outcome, lease=lease)

    pool_spy.resume = resume
    t = await inbox.raise_ask("run_p", draft())
    await inbox.answer(t["ask_id"], choice="A", raw_text="A", via=CH)
    await c.tick()  # the resume starts and waits inside the pool
    assert c.dispatch.work_full()  # agenda run + resume = the 2-slot cap
    ready_item(projects, pid, title="B사 가격표")
    await c.tick()
    assert len(pool_spy.runs) == 1  # no second agenda run while the resume is live
    finish.set()
    for _ in range(100):
        if not c.dispatch.work_full():
            break
        await asyncio.sleep(0)
    assert not c.dispatch.work_full()
    await c.tick()
    assert len(pool_spy.runs) == 2
    pool_spy.gate.set()
    await c.drain()
    await c.aclose()


async def test_logged_out_engine_makes_the_agenda_sleep_and_status_say_so(
    coordinator, stack, projects, pid, fake_brain, clock, log
):
    """auth_failed disables leases; the agenda sleeps instead of deferring each tick."""
    ready_item(projects, pid)
    stack["ledger"].observe(
        engine="claude", signal=UsageSignal(kind="auth_failed", reset_at=None, at=clock.now())
    )
    await coordinator.tick()
    assert [t["payload"]["kind"] for t in kinds(log, "agenda.tick")] == ["sleep"]
    assert kinds(log, "agenda.deferred") == []
    assert "로그인 필요" in await coordinator.status(CH)


async def test_failed_consolidation_does_not_spin_the_loop(stack, pid, projects, fake_brain, log):
    """A consolidation that fails at once (no Brain lease) is not re-dispatched by
    the wake its own failure causes; it waits CONSOLIDATE_RETRY."""
    dispatched(log, pid, n=30)
    for t in ("a", "b"):
        projects.record_learning(pid, {"text": t, "evidence": []})
    fake_brain.returns(BrainError("no claude slot free"), "report")
    c = stack["make"](tick_every_s=0.01)
    await run_loop_until(c, lambda: False, real_s=0.3)
    assert len(kinds(log, "coordinator.consolidate_failed")) == 1


async def test_answer_is_not_lost_when_persisting_the_resume_fails(
    coordinator, inbox, pool_spy, pid, stack, monkeypatch
):
    """drain_closed and the coordinator_resumes insert commit together."""
    pool_spy.add_active("run_w", pid, status="waiting_human")
    t = await inbox.raise_ask("run_w", draft())
    await inbox.answer(t["ask_id"], choice="A", raw_text="A", via=CH)
    db = stack["db"]
    real = db.execute
    fail = [True]

    def flaky(sql, params=()):
        if fail and sql.startswith("INSERT OR IGNORE INTO coordinator_resumes"):
            fail.clear()
            raise RuntimeError("disk I/O error")
        return real(sql, params)

    monkeypatch.setattr(db, "execute", flaky)
    await coordinator.tick()  # the drain phase fails
    await coordinator.drain()
    assert pool_spy.resumes == []
    await coordinator.tick()
    await coordinator.drain()
    assert [r["run_id"] for r in pool_spy.resumes] == ["run_w"]


async def test_watchdog_stuck_cancel_tells_the_project(
    coordinator, projects, pid, pool_spy, notifier
):
    """A run the watchdog cancelled as stuck blocks its item with one FYI."""
    ready_item(projects, pid)
    pool_spy.result = lambda order, rid: result(rid, status="cancelled", summary="cancelled: stuck")
    await coordinator.tick()
    await coordinator.drain()
    assert projects.get(pid)["backlog"][0]["status"] == "blocked"
    [m] = notifier.sent
    assert m["kind"] == "fyi" and m["channel"] == CH and "멈춘 작업" in m["text"]


async def test_resume_deferred_is_logged_once_per_cooling(
    coordinator, inbox, pool_spy, pid, stack, clock, log
):
    """A resume waiting out a cooling is logged once, not on every tick."""
    pool_spy.add_active("run_w", pid, status="waiting_human")
    t = await inbox.raise_ask("run_w", draft())
    await inbox.answer(t["ask_id"], choice="A", raw_text="A", via=CH)
    stack["ledger"].observe(
        engine="claude",
        signal=UsageSignal(kind="limit_hit", reset_at=clock.now() + timedelta(hours=5),
                           at=clock.now()),
    )  # fmt: skip
    for _ in range(5):
        await coordinator.tick()
        clock.advance(timedelta(minutes=1))
    assert len(kinds(log, "coordinator.resume_deferred")) == 1
    clock.advance(timedelta(hours=5))
    await coordinator.tick()
    await coordinator.drain()
    assert [r["run_id"] for r in pool_spy.resumes] == ["run_w"]


def test_retry_ladders_keep_each_callers_schedule():
    # resume and confirmation retry: attempts counts from 1, the 5th failure gives up
    assert [RESUME_BACKOFF.after(a) for a in range(1, 7)] == [60, 300, 900, 3600, None, None]
    # held tell: `attempt` counts retries already made (from 0), the 4th gives up
    assert [HELD_BACKOFF.after(a + 1) for a in range(5)] == [60, 300, 900, None, None]
    # item interrupts: the 3rd in a row stops the item
    assert [ITEM_BACKOFF.after(n) for n in range(1, 5)] == [60, 300, None, None]
    assert ITEM_STOP_AFTER == 3
    # failed report: 5m, 15m, then hourly forever
    assert [REPORT_BACKOFF.capped(f) for f in range(1, 7)] == [300, 900, 3600, 3600, 3600, 3600]
