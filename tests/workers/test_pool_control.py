"""Run control is serialized per run; RunResult status stays frozen."""

import asyncio

import pytest

from swarm.harness.base import Done
from swarm.types import Evidence
from swarm.workers.verify import VerifierUnavailable

from .conftest import BLOCK, V_OWN, order
from .test_pool_approval import ASK as APPROVAL_ASK
from .test_pool_approval import ask, block
from .test_pool_approval import outcome as human
from .test_pool_resume import done_script, outcome, park, pending, settle


async def test_takeover_during_resume_prelaunch_await(fakes, pool):
    """Reviewer probe: resume waits on a 2 h teardown in flight, takeover lands meanwhile."""
    rid = await park(fakes, pool)
    gate = asyncio.Event()
    fakes.guard.disarm_gate = gate
    fakes.guard.disarm_entered.clear()
    fakes.sleeper.open()  # 2 h timer fires -> teardown blocks in disarm
    await fakes.guard.disarm_entered.wait()
    fakes.harness.scripts.append([BLOCK])
    resuming = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    await settle()
    take = asyncio.create_task(pool.takeover(rid))
    await settle()
    gate.set()
    info = await take
    r = await resuming
    await settle()
    assert r["status"] == "waiting_human"
    opened = [s["id"] for s in fakes.browser.opened]
    assert opened == ["bs_1", "bs_2"] and info["target_id"] == "T1"
    assert fakes.harness.closed  # the resumed worker was paused, not left driving
    assert fakes.harness.seen_guard == ["guarded", "guarded"]  # never ran with guard off
    assert pool.session(rid)["id"] == "bs_2" and pool.session(rid)["guard"] == "off"
    row = fakes.db.execute("SELECT status, taken_over FROM runs WHERE id=?", (rid,)).fetchone()
    assert tuple(row) == ("waiting_human", 1)
    await pool.cancel(rid, reason="t")


async def test_two_concurrent_resumes_one_driver(fakes, pool):
    rid = await park(fakes, pool)
    go = asyncio.Event()

    async def hold(sink):
        await go.wait()

    def ev(sink):
        pool.add_evidence(
            sink.run_id,
            Evidence(
                url="http://c.test/", screenshot=None, captured_at=fakes.clock.now(), text="t"
            ),
        )

    fakes.harness.started.clear()
    fakes.harness.scripts.append([hold, ev, Done(summary="ok")])
    r1 = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    r2 = asyncio.create_task(
        pool.resume(rid, outcome=outcome(rid, "C", ask_id="ask_2"), lease=fakes.lease())
    )
    await fakes.harness.started.wait()
    await settle()
    assert "사람 답: C" in pending(fakes, rid)
    go.set()
    a, b = await r1, await r2
    assert a["status"] == b["status"] == "done"  # frozen statuses only, same drive
    assert len(fakes.harness.tasks) == 2  # initial run + exactly one resume
    dropped = [e for e in fakes.log.query(run_id=rid, kinds=["run.pending_dropped"])]
    assert dropped and dropped[0]["payload"]["lines"] == ["사람 답: C"]


async def test_row_result_never_reports_running(fakes, pool):
    fakes.harness.scripts.append([BLOCK])
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    fresh = fakes.pool()  # restarted instance before recover_on_boot: no live driver
    assert (await fresh.wait(rid))["status"] == "interrupted"
    r = await fresh.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "interrupted"
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_grant_orphaned_when_resume_fails_after_grant(fakes, pool, monkeypatch):
    def failing_revoke(ask_id):
        raise RuntimeError("revoke failed")

    fakes.guard.revoke = failing_revoke
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])

    async def boom(rec, line):
        raise RuntimeError("prepare failed")

    monkeypatch.setattr(pool, "_prepare_resume", boom)
    fakes.ledger.released.clear()
    try:
        await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    except RuntimeError:
        pass
    assert len(fakes.guard.granted) == 1
    assert "guard.grant_orphaned" in fakes.kinds(rid)
    assert fakes.ledger.released == [fakes.lease()]


async def test_grant_revoked_when_guard_supports_it(fakes, pool, monkeypatch):
    revoked = []
    fakes.guard.revoke = lambda ask_id: revoked.append(ask_id)
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])

    async def boom(rec, line):
        raise RuntimeError("prepare failed")

    monkeypatch.setattr(pool, "_prepare_resume", boom)
    try:
        await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    except RuntimeError:
        pass
    assert revoked == ["ask_1"] and "guard.grant_orphaned" not in fakes.kinds(rid)


def with_revoke(fakes):
    revoked: list[str] = []

    def revoke(ask_id):
        if ask_id in [t.ask_id for t in fakes.guard.granted] and ask_id not in revoked:
            revoked.append(ask_id)
            return True
        return False

    fakes.guard.revoke = revoke
    return revoked


async def test_unused_grant_revoked_when_run_is_cancelled(fakes, pool):
    revoked = with_revoke(fakes)
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])
    fakes.harness.started.clear()
    fakes.harness.scripts.append([BLOCK])  # the worker never sends the approved request
    resuming = asyncio.create_task(
        pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    )
    await fakes.harness.started.wait()
    assert len(fakes.guard.granted) == 1 and revoked == []
    await pool.cancel(rid, reason="human")
    assert (await resuming)["status"] == "cancelled"
    assert revoked == ["ask_1"]
    (ev,) = fakes.log.query(run_id=rid, kinds=["guard.grant_revoked"])
    assert ev["payload"]["ask_id"] == "ask_1"


async def test_unused_grant_revoked_on_done_too(fakes, pool):
    revoked = with_revoke(fakes)
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])

    def ev(sink):
        pool.add_evidence(
            sink.run_id,
            Evidence(
                url="http://c.test/", screenshot=None, captured_at=fakes.clock.now(), text="t"
            ),
        )

    fakes.harness.scripts.append([ev, Done(summary="ok")])
    r = await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    assert r["status"] == "done" and revoked == ["ask_1"]


async def test_cancel_reason_visible_while_lock_is_held(fakes, pool):
    rid = await park(fakes, pool)
    async with pool._lock(rid):  # a slow takeover/resume holder
        canceller = asyncio.create_task(pool.cancel(rid, reason="human"))
        await settle()
        assert pool._live[rid].cancel_reason == "human"
        assert not canceller.done()
    await canceller
    assert pool.get(rid)["status"] == "cancelled"


async def test_follower_resume_survives_foreign_cancel_of_the_drive(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.started.clear()
    fakes.harness.scripts.append([BLOCK])
    first = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    await fakes.harness.started.wait()
    follower = asyncio.create_task(
        pool.resume(rid, outcome=outcome(rid, "C", ask_id="ask_2"), lease=fakes.lease())
    )
    await settle()
    first.cancel()  # e.g. shutdown cancels the resume caller, and with it the drive
    try:
        await first
    except asyncio.CancelledError:
        pass
    r = await follower
    assert r["status"] == "interrupted"


async def test_locks_pruned_for_finished_runs(fakes, pool):
    rid = await park(fakes, pool)
    await pool.cancel(rid, reason="t")
    assert rid not in pool._locks


# --- lock pruning, cancel liveness, grant bookkeeping -------------------------------------


async def test_lock_pruned_after_resume_drive_finishes(fakes, pool):
    rid = await park(fakes, pool)
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"
    assert rid not in pool._locks and rid not in pool._lock_users


async def test_lock_kept_while_a_waiter_is_queued(fakes, pool):
    rid = await park(fakes, pool)
    go = asyncio.Event()

    async def hold(sink):
        await go.wait()

    fakes.harness.started.clear()
    fakes.harness.scripts.append([hold, *done_script(fakes, pool)])
    resuming = asyncio.create_task(pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease()))
    await fakes.harness.started.wait()
    lock = pool._locks[rid]

    async def waiter():
        async with pool._control(rid):
            pass

    async with pool._control(rid):
        w = asyncio.create_task(waiter())
        await settle()
        go.set()
        assert (await resuming)["status"] == "done"
        assert pool._locks.get(rid) is lock  # a queued waiter must find the same lock
    await w
    assert rid not in pool._locks and rid not in pool._lock_users


async def test_cancel_cancelled_while_waiting_clears_its_reason(fakes, pool):
    rid = await park(fakes, pool)
    async with pool._control(rid):  # a slow takeover/resume holder
        canceller = asyncio.create_task(pool.cancel(rid, reason="human"))
        await settle()
        assert pool._live[rid].cancel_reason == "human"
        canceller.cancel()
        try:
            await canceller
        except asyncio.CancelledError:
            pass
        assert pool._live[rid].cancel_reason is None
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=outcome(rid, "B"), lease=fakes.lease())
    assert r["status"] == "done"  # not refused forever by a stale cancel reason


async def test_cancelled_second_canceller_keeps_the_first_reason(fakes, pool):
    rid = await park(fakes, pool)
    async with pool._control(rid):
        first = asyncio.create_task(pool.cancel(rid, reason="first"))
        await settle()
        second = asyncio.create_task(pool.cancel(rid, reason="second"))
        await settle()
        second.cancel()
        try:
            await second
        except asyncio.CancelledError:
            pass
        assert pool._live[rid].cancel_reason == "first"
    await first
    assert pool.get(rid)["status"] == "cancelled"


async def test_drop_grant_emits_revoked_only_when_a_token_was_removed(fakes, pool, monkeypatch):
    fakes.guard.revoke = lambda ask_id: False  # already used / unknown: nothing removed
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])

    async def boom(rec, line):
        raise RuntimeError("prepare failed")

    monkeypatch.setattr(pool, "_prepare_resume", boom)
    try:
        await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    except RuntimeError:
        pass
    kinds = fakes.kinds(rid)
    assert "guard.grant_revoked" not in kinds and "guard.grant_orphaned" not in kinds


async def test_finish_revokes_every_granted_token(fakes, pool):
    revoked: list[str] = []

    def revoke(ask_id):
        revoked.append(ask_id)
        return True

    fakes.guard.revoke = revoke
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])
    fakes.harness.started.clear()
    fakes.harness.scripts.append([BLOCK])
    resuming = asyncio.create_task(
        pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    )
    await fakes.harness.started.wait()
    await pool.cancel(rid, reason="human")
    assert (await resuming)["status"] == "cancelled"
    assert revoked == ["ask_1"]
    (ev,) = fakes.log.query(run_id=rid, kinds=["guard.grant_revoked"])
    assert ev["payload"] == {"ask_id": "ask_1", "why": "cancelled"}


async def test_unused_grant_revoked_on_interrupted(fakes, pool):
    revoked = with_revoke(fakes)
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])

    async def down(order, artifacts, evidence):
        raise VerifierUnavailable("cli down")

    fakes.verifier.verify = down
    fakes.harness.scripts.append(done_script(fakes, pool))
    r = await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    assert r["status"] == "interrupted" and revoked == ["ask_1"]
    (ev,) = fakes.log.query(run_id=rid, kinds=["guard.grant_revoked"])
    assert ev["payload"]["why"] == "interrupted"


async def test_raising_revoke_does_not_break_the_finish(fakes, pool):
    def revoke(ask_id):
        raise RuntimeError("guard gone")

    fakes.guard.revoke = revoke
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])
    fakes.harness.started.clear()
    fakes.harness.scripts.append([BLOCK])
    resuming = asyncio.create_task(
        pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    )
    await fakes.harness.started.wait()
    await pool.cancel(rid, reason="human")
    assert (await resuming)["status"] == "cancelled"
    assert "guard.grant_revoked" not in fakes.kinds(rid)
    assert fakes.browser.closed == ["bs_1"] and fakes.ledger.released


async def test_grant_tracked_before_its_event_is_emitted(fakes, pool, monkeypatch):
    revoked = with_revoke(fakes)
    rid = await park(fakes, pool, script=[block(fakes), ask(), *APPROVAL_ASK])
    emit = pool._emit

    def flaky(kind, payload=None, /, **kw):
        if kind == "run.approval_granted":
            raise RuntimeError("log full")
        emit(kind, payload, **kw)

    monkeypatch.setattr(pool, "_emit", flaky)
    with pytest.raises(RuntimeError):
        await pool.resume(rid, outcome=human(rid, "승인"), lease=fakes.lease())
    assert len(fakes.guard.granted) == 1 and pool._live[rid].granted == ["ask_1"]
    monkeypatch.setattr(pool, "_emit", emit)
    await pool.cancel(rid, reason="t")
    assert revoked == ["ask_1"]  # the live token never outlives the run
