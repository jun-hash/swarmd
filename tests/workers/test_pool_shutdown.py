"""WorkerPool.aclose: live drivers end interrupted, and every pool-owned
background task (teardown, canceller, releaser, park timer) is gathered before the daemon
closes the browser."""

import asyncio

import pytest

from swarm.harness.base import AskRaised, Done

from .conftest import BLOCK, V_OWN, order


async def w(aw, timeout=5.0):
    return await asyncio.wait_for(aw, timeout)


async def settle():
    for _ in range(5):
        await asyncio.sleep(0)


@pytest.fixture
def gate(fakes):
    """A disarm gate that is always opened at the end, so a failing test never hangs."""
    fakes.guard.disarm_gate = asyncio.Event()
    fakes.guard.disarm_gate.set()
    yield fakes.guard
    fakes.guard.disarm_gate.set()


async def test_aclose_interrupts_running_runs_and_closes_their_sessions(fakes, pool, gate):
    fakes.harness.script = [BLOCK]
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await w(fakes.harness.started.wait())
    run_id = pool.running()[0]["id"]
    gate.disarm_gate.clear()  # teardown still in flight when aclose starts
    closing = asyncio.create_task(pool.aclose())
    await w(gate.disarm_entered.wait())
    await settle()
    assert not closing.done()  # waits for the shielded teardown
    gate.disarm_gate.set()
    await w(closing)
    assert pool.get(run_id)["status"] == "interrupted"
    assert fakes.browser.closed == ["bs_1"] and fakes.ledger.released == [fakes.lease()]
    assert pool.background_tasks() == []
    await asyncio.gather(task, return_exceptions=True)


async def test_aclose_closes_parked_sessions_and_stops_their_timers(fakes, pool, gate):
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="asked")]
    r = await w(pool.run(order(), V_OWN, lease=fakes.lease()))
    await settle()
    await w(pool.aclose())
    assert pool.get(r["run_id"])["status"] == "waiting_human"  # still waits for the human
    assert fakes.browser.closed == ["bs_1"] and fakes.guard.disarmed == ["bs_1"]
    assert pool.background_tasks() == []


async def test_aclose_gathers_a_cancel_cleanup_in_flight(fakes, pool, gate):
    fakes.harness.script = [AskRaised(question="A?", irreversible=False), Done(summary="asked")]
    r = await w(pool.run(order(), V_OWN, lease=fakes.lease()))
    await settle()
    gate.disarm_gate.clear()
    canceller = asyncio.create_task(pool.cancel(r["run_id"], reason="human"))
    await w(gate.disarm_entered.wait())
    canceller.cancel()  # the caller went away; the shielded canceller keeps going
    await asyncio.gather(canceller, return_exceptions=True)
    assert pool.background_tasks()
    closing = asyncio.create_task(pool.aclose())
    await settle()
    assert not closing.done()
    gate.disarm_gate.set()
    await w(closing)
    assert pool.get(r["run_id"])["status"] == "cancelled"
    assert pool.background_tasks() == []
