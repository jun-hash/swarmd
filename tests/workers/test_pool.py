import asyncio
from datetime import timedelta

import pytest

from swarm.harness.base import (
    AskRaised,
    ContextRollover,
    Done,
    Error,
    FileOutput,
    QuotaSignal,
    ToolCall,
    TurnUsage,
)
from swarm.types import FailedAttempt, Variant
from swarm.workers.contract import (
    IncompatibleVariant,
    InvalidOrder,
    LeaseExpired,
    UnknownProfile,
)

from .conftest import BLOCK, V_OWN, FakeRegistry, add_evidence, evidence, order


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


# --- validation -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "kw",
    [
        {"intent": ""},
        {"intent": "x" * 2001},
        {"done_criteria": []},
        {"done_criteria": [f"c{i}" for i in range(13)]},
        {"done_criteria": ["ok", "  "]},
        {"budget": {"max_minutes": 0}},
        {"budget": {"max_minutes": 241}},
        {"kind": "advance", "project_id": None},
    ],
)
async def test_invalid_order_raises_before_run(fakes, pool, kw):
    with pytest.raises(InvalidOrder):
        await pool.run(order(**kw), V_OWN, lease=fakes.lease())
    assert fakes.browser.opened == [] and pool.running() == []


async def test_incompatible_variant(fakes, pool):
    v = Variant(
        harness="browsercode", engine="claude", browser="chrome", context_policy="", model=None
    )
    with pytest.raises(IncompatibleVariant):
        await pool.run(order(), v, lease=fakes.lease())


async def test_compatibility_comes_from_the_pool_s_table(fakes):
    from swarm.workers.pool import WorkerPool

    assert await fakes.pool().run(order(), V_OWN, lease=fakes.lease())  # in the table
    empty = WorkerPool(
        db=fakes.db, projects=fakes.projects, inbox=fakes.inbox, ledger=fakes.ledger,
        engines={"claude": fakes.engine}, compatible=(), browsers={"chrome": fakes.browser},
        guards=fakes.guards, harnesses=FakeRegistry(fakes.harness), log=fakes.log,
        clock=fakes.clock, output_root=fakes.tmp_path / "runs",
    )  # fmt: skip
    with pytest.raises(IncompatibleVariant):
        await empty.run(order(id="ord_2"), V_OWN, lease=fakes.lease())


async def test_unknown_profile(fakes, pool):
    with pytest.raises(UnknownProfile):
        await pool.run(order(profile="nope"), V_OWN, lease=fakes.lease())


async def test_lease_expired_or_missing(fakes, pool):
    with pytest.raises(LeaseExpired):
        await pool.run(order(), V_OWN, lease=fakes.lease(minutes=0))
    with pytest.raises(LeaseExpired):
        await pool.run(order(), V_OWN, lease=None)
    assert fakes.browser.opened == []


# --- run ------------------------------------------------------------------------------


async def test_arm_happens_before_harness_start(fakes, pool):
    fakes.harness.script = [add_evidence(fakes, pool), Done(summary="ok")]
    await pool.run(order(action_mode="dry_run"), V_OWN, lease=fakes.lease())
    assert fakes.trace.index("guard.arm:dry_run") < fakes.trace.index("harness.start")
    assert fakes.harness.seen_guard == ["dry_run"]


async def test_harness_task_built_from_project(fakes, pool):
    fakes.projects.record_failure(
        "prj_compet", FailedAttempt(what="가격 페이지 직접 접근", why="로그인 벽", run_id=None)
    )
    await pool.run(order(), V_OWN, lease=fakes.lease())
    t = fakes.harness.tasks[0]
    assert t["instruction"] == order()["intent"]
    assert t["done_criteria"] == order()["done_criteria"]
    assert t["action_mode"] == "guarded" and t["budget"] == {"max_minutes": 60}
    assert "C사 중심" in t["memory_brief"]
    assert t["recent_failures"] == ["가격 페이지 직접 접근 -> 로그인 벽"]


async def test_done_requires_verified_artifacts_with_evidence(fakes, pool):
    fakes.harness.script = [
        add_evidence(fakes, pool),
        FileOutput(path="/tmp/shot.png", kind="screenshot"),
        TurnUsage(input_tokens=10, output_tokens=5, cached_tokens=0, cost_usd=0.01),
        Done(summary="가격은 ₩9,900로 바뀌었다. 상세는 첨부."),
    ]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "done" and r["failure"] is None
    assert r["artifacts"]
    assert all(
        a["status"] == "verified" and a["evidence"] and a["verifier_run_id"] for a in r["artifacts"]
    )
    assert r["summary"].startswith("가격은") and len(r["summary"]) <= 600
    assert r["cost"]["turns"] == 1 and r["cost"]["engine_usage"]["output_tokens"] == 5
    assert pool.get(r["run_id"])["status"] == "done"
    stored = pool.result(r["run_id"])
    assert stored is not None and stored["summary"] == r["summary"]
    assert [a["path"] for a in stored["artifacts"]] == [a["path"] for a in r["artifacts"]]
    assert pool.result("run_nope") is None
    kinds = fakes.kinds(r["run_id"])
    assert "run.started" in kinds and "run.verifying" in kinds
    assert kinds.index("run.verifying") < kinds.index("run.done") < kinds.index("run.finished")
    # lease released, guard disarmed before the session is closed
    assert fakes.ledger.released == [fakes.lease()]
    assert fakes.trace[-2:] == ["guard.disarm", "browser.close"]


async def test_done_without_evidence_fails(fakes, pool):
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed"
    assert r["failure"] and "no evidence" in r["failure"]["why"]
    assert all(a["status"] != "verified" for a in r["artifacts"])
    fails = fakes.projects.get("prj_compet")["memory"]["failed_attempts"]
    assert fails[-1]["run_id"] == r["run_id"]


async def test_waiting_human_releases_lease_and_keeps_session(fakes, pool):
    fakes.harness.script = [
        AskRaised(question="A or B?", irreversible=False, number=7),
        Done(summary="asked"),
    ]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    await settle()
    assert r["status"] == "waiting_human"
    assert fakes.ledger.released == [fakes.lease()]
    assert fakes.browser.closed == [] and fakes.guard.disarmed == []
    assert fakes.sleeper.calls == [7200]
    kinds = fakes.kinds(r["run_id"])
    assert "run.waiting_human" in kinds and "run.parked" in kinds and "run.finished" not in kinds
    assert pool.running()[0]["status"] == "waiting_human"


async def test_parked_session_closed_after_2h(fakes, pool):
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="asked")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    await settle()
    fakes.clock.advance(timedelta(hours=2))
    fakes.sleeper.open()
    await settle()
    assert fakes.guard.disarmed == ["bs_1"] and fakes.browser.closed == ["bs_1"]
    assert pool.get(r["run_id"])["status"] == "waiting_human"
    assert pool.session(r["run_id"]) is None
    assert "run.session_closed" in fakes.kinds(r["run_id"])


async def test_quota_signal_interrupts_not_fails(fakes, pool):
    sig = {"kind": "limit_hit", "reset_at": None, "at": fakes.clock.now()}
    fakes.harness.script = [QuotaSignal(signal=sig), Error(message="quota:limit_hit")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "interrupted" and r["failure"] is None
    assert fakes.ledger.observed == [("claude", sig)]
    assert fakes.ledger.released == [fakes.lease()]
    assert fakes.projects.get("prj_compet")["memory"]["failed_attempts"] == []
    assert fakes.browser.closed == ["bs_1"]


async def test_ask_then_error_expires_asks_and_fails(fakes, pool):
    fakes.sleeper.open()  # retry ladder: resume now, fresh session after 60 s, then failed
    fakes.harness.scripts = [
        [AskRaised(question="A?", irreversible=False), Error(message="engine crashed")],
        [Error(message="engine crashed")],
        [Error(message="engine crashed")],
    ]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert fakes.inbox.expired == [r["run_id"]]
    assert r["status"] == "failed" and "engine crashed" in r["failure"]["why"]
    assert fakes.browser.closed == ["bs_1"]


async def test_other_error_fails_and_records_failure(fakes, pool):
    fakes.sleeper.open()  # after the retry ladder
    fakes.harness.script = [ToolCall(id="t", name="x", args={}), Error(message="boom")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and r["failure"]["why"] == "boom"
    assert fakes.inbox.expired == []
    fails = fakes.projects.get("prj_compet")["memory"]["failed_attempts"]
    assert fails[-1]["why"] == "boom"
    assert fakes.ledger.released == [fakes.lease()]


async def test_browser_failure_interrupts_and_releases(fakes, pool):
    fakes.browser.fail_open = True
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "interrupted"
    assert fakes.ledger.released == [fakes.lease()]
    assert "harness.start" not in fakes.trace


async def test_sink_records_and_checkpoint_persisted(fakes, pool):
    def rollover(sink):
        sink.record(
            ContextRollover(
                policy="rollover",
                prev_session_id="s1",
                reason="r",
                injected_chars=10,
                injected_tokens=3,
            )
        )

    fakes.harness.script = [rollover, add_evidence(fakes, pool), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    kinds = fakes.kinds(r["run_id"])
    assert "ctx.rollover" in kinds and "harness.checkpoint" in kinds
    assert pool.checkpoint(r["run_id"]) == {
        "session_id": "sess-9",
        "notes": ["n"],
        "open_tabs": ["http://a/"],
        "pending_steers": [],
    }
    # survives a restart (persisted as JSON)
    assert fakes.pool().checkpoint(r["run_id"])["session_id"] == "sess-9"


async def test_sink_raise_ask_goes_to_inbox(fakes, pool):
    async def ask(sink):
        await sink.raise_ask(
            {
                "kind": "decision",
                "question": "A?",
                "options": ["A"],
                "recommended": None,
                "default_after": None,
                "irreversible": False,
                "evidence": [],
            }
        )

    fakes.harness.script = [ask, Done(summary="x")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert fakes.inbox.asks[0][0] == r["run_id"]


async def test_run_project_and_running(fakes, pool):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    [run] = pool.running()
    assert run["status"] == "running" and run["order_id"] == "ord_1"
    assert pool.run_project(run["id"]) == "prj_compet"
    assert pool.run_project("run_nope") is None
    assert pool.order(run["id"])["id"] == "ord_1" and pool.order("run_nope") is None
    await pool.cancel(run["id"], reason="test")
    await task


# --- cancel ---------------------------------------------------------------------------


async def test_cancel_running_run(fakes, pool):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    run_id = pool.running()[0]["id"]
    await pool.cancel(run_id, reason="priority change")
    r = await task
    assert r["status"] == "cancelled" and fakes.harness.closed
    assert fakes.inbox.expired == [run_id]
    assert fakes.guard.disarmed == ["bs_1"] and fakes.browser.closed == ["bs_1"]
    assert fakes.ledger.released == [fakes.lease()]
    assert pool.get(run_id)["status"] == "cancelled" and pool.running() == []


async def test_cancel_parked_run(fakes, pool):
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="asked")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    await settle()
    await pool.cancel(r["run_id"], reason="human")
    assert pool.get(r["run_id"])["status"] == "cancelled"
    assert fakes.inbox.expired == [r["run_id"]]
    assert fakes.browser.closed == ["bs_1"] and fakes.guard.disarmed == ["bs_1"]
    fakes.sleeper.open()  # the 2h timer is gone: nothing closes twice
    await settle()
    assert fakes.browser.closed == ["bs_1"]
    assert fakes.ledger.released == [fakes.lease()]  # released once at park


async def test_cancel_finished_run_is_noop(fakes, pool):
    fakes.harness.script = [add_evidence(fakes, pool), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    await pool.cancel(r["run_id"], reason="late")
    assert pool.get(r["run_id"])["status"] == "done"


# --- state machine ------------------------------------------------------------------------


async def test_recover_on_boot_marks_interrupted(fakes, pool):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="a")]
    parked = (await pool.run(order(id="ord_2"), V_OWN, lease=fakes.lease()))["run_id"]
    fresh = fakes.pool()  # a new swarmd instance over the same db
    assert fresh.recover_on_boot() == [rid]
    assert fresh.get(rid)["status"] == "interrupted"
    assert fresh.get(parked)["status"] == "waiting_human"
    assert "run.interrupted" in fakes.kinds(rid)
    assert fresh.recover_on_boot() == []
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# --- exception safety, shielded teardown, concurrent cancel -------------------------------


async def test_unknown_project_is_invalid_order(fakes, pool):
    with pytest.raises(InvalidOrder):
        await pool.run(order(project_id="prj_nope"), V_OWN, lease=fakes.lease())
    assert fakes.db.execute("SELECT count(*) FROM runs").fetchone()[0] == 0
    assert fakes.browser.opened == []


async def test_raising_verify_fails_and_tears_down(fakes, pool, monkeypatch):
    async def boom(order, artifacts, evidence):
        raise RuntimeError("verifier exploded")

    monkeypatch.setattr(fakes.verifier, "verify", boom)
    fakes.harness.script = [add_evidence(fakes, pool), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and "verifier exploded" in r["failure"]["why"]
    assert pool.get(r["run_id"])["status"] == "failed"
    assert fakes.guard.disarmed == ["bs_1"] and fakes.browser.closed == ["bs_1"]
    assert fakes.ledger.released == [fakes.lease()]
    fails = fakes.projects.get("prj_compet")["memory"]["failed_attempts"]
    assert fails[-1]["run_id"] == r["run_id"]


async def test_raising_event_handler_fails_and_tears_down(fakes, pool, monkeypatch):
    def boom(rec, ev):
        raise ValueError("bad event")

    monkeypatch.setattr(pool, "_on_event", boom)
    fakes.harness.script = [ToolCall(id="t", name="x", args={}), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["status"] == "failed" and fakes.browser.closed == ["bs_1"]
    assert fakes.harness.closed


async def test_cancel_during_disarm_still_closes_session(fakes, pool):
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="asked")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    await settle()
    fakes.guard.disarm_gate = asyncio.Event()
    canceller = asyncio.create_task(pool.cancel(r["run_id"], reason="human"))
    await fakes.guard.disarm_entered.wait()
    canceller.cancel()  # the cancel() caller itself goes away mid-teardown
    with pytest.raises(asyncio.CancelledError):
        await canceller
    assert pool.session(r["run_id"]) is None  # teardown in progress: not handed out
    fakes.sleeper.open()  # park timer fires now: must not close a second time
    fakes.guard.disarm_gate.set()
    await settle()
    assert fakes.browser.closed == ["bs_1"] and fakes.guard.disarmed == ["bs_1"]


async def test_two_concurrent_cancels(fakes, pool):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    fakes.guard.disarm_gate = asyncio.Event()
    c1 = asyncio.create_task(pool.cancel(rid, reason="one"))
    await fakes.guard.disarm_entered.wait()
    c2 = asyncio.create_task(pool.cancel(rid, reason="two"))
    await settle()
    fakes.guard.disarm_gate.set()
    await asyncio.gather(c1, c2)
    r = await task  # no stray CancelledError
    assert r["status"] == "cancelled" and r["summary"] == "cancelled: one"
    assert fakes.browser.closed == ["bs_1"] and fakes.guard.disarmed == ["bs_1"]
    assert pool.get(rid)["status"] == "cancelled"


async def test_cancel_does_not_swallow_own_cancellation(fakes, pool):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    fakes.guard.disarm_gate = asyncio.Event()
    c = asyncio.create_task(pool.cancel(rid, reason="x"))
    await fakes.guard.disarm_entered.wait()
    c.cancel()
    with pytest.raises(asyncio.CancelledError):
        await c
    fakes.guard.disarm_gate.set()
    r = await task
    assert r["status"] == "cancelled" and fakes.browser.closed == ["bs_1"]


async def test_finished_runs_pruned_from_live(fakes, pool):
    fakes.harness.script = [add_evidence(fakes, pool), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert r["run_id"] not in pool._live


async def test_evidence_id_is_kept(fakes, pool):
    def hook(sink):
        pool.add_evidence(sink.run_id, {**evidence(fakes), "id": "ev_1"})  # type: ignore[typeddict-unknown-key]

    fakes.harness.script = [hook, Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    assert [e.get("id") for e in pool.evidence(r["run_id"])] == ["ev_1"]


async def test_guard_dedupe_is_scoped_to_the_backlog_item(fakes, pool):
    """Two orders (a re-dispatch) for one backlog item share the guard's dedupe key;
    an order without an item keeps its own id."""
    await pool.run(order(id="wo_A", backlog_item_id="bl_1"), V_OWN, lease=fakes.lease())
    await pool.run(order(id="wo_B", backlog_item_id="bl_1"), V_OWN, lease=fakes.lease())
    await pool.run(order(id="wo_C"), V_OWN, lease=fakes.lease())
    assert fakes.guard.dedupe_keys == ["item:bl_1", "item:bl_1", "wo_C"]
