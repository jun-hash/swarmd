"""SWARM_SIMULATE_LIMIT=1 (soak): the first engine call after boot reports a usage
limit through the same CLI result text and parser as a real one; inert when the env is unset."""

from datetime import datetime, timedelta

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.engine.base import EngineError, Error, QuotaSignal
from swarm.engine.fake import FakeEngine
from swarm.engine.simulate import ENV, LimitSimulatingEngine, wrap_limit_simulation
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog

TURN = {"prompt": "hi", "mcp_config": {}, "resume_id": None, "max_minutes": 5}
NOW = datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL)


# Overrides the root clock: tests here count from the module's NOW.
@pytest.fixture
def clock():
    return FakeClock(NOW)


@pytest.mark.parametrize("env", [{}, {ENV: ""}, {ENV: "0"}, {"OTHER": "1"}])
def test_inert_unless_env_is_1(env, clock):
    engine = FakeEngine([])
    assert wrap_limit_simulation(engine, clock, env=env) is engine


async def test_first_start_reports_limit_then_passes_through(clock):
    inner = FakeEngine([Error(message="real")])
    engine = wrap_limit_simulation(inner, clock, env={ENV: "1"})
    assert isinstance(engine, LimitSimulatingEngine) and engine.id == inner.id

    run = await engine.start(TURN)
    evs = [e async for e in run.events()]
    assert inner.turns == []  # the real engine was never started
    assert isinstance(evs[0], QuotaSignal)
    sig = evs[0].signal
    assert sig["kind"] == "limit_hit" and sig["reset_at"] is None and sig["at"] == NOW
    assert evs[1:] == [Error(message="quota:limit_hit")]  # the pool's "quota:*" -> interrupted
    assert run.session_id is None
    await run.interrupt()

    run2 = await engine.start(TURN)
    assert [e async for e in run2.events()] == [Error(message="real")]
    assert len(inner.turns) == 1


async def test_first_single_shot_raises_limit_then_passes_through(clock):
    inner = FakeEngine([])
    inner.single_shot_returns({"ok": True})
    engine = wrap_limit_simulation(inner, clock, env={ENV: "1"})
    with pytest.raises(EngineError) as e:
        await engine.single_shot(prompt="p", json_schema={}, timeout_s=5)
    assert e.value.signal is not None and e.value.signal["kind"] == "limit_hit"
    assert inner.single_shot_calls == []
    assert await engine.single_shot(prompt="p", json_schema={}, timeout_s=5) == {"ok": True}


async def test_ledger_cools_the_engine_for_an_hour_on_the_simulated_signal(clock, tmp_path):
    db = Database(tmp_path / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    engine = wrap_limit_simulation(FakeEngine([]), clock, env={ENV: "1"})
    run = await engine.start(TURN)
    sig = [e async for e in run.events() if isinstance(e, QuotaSignal)][0].signal

    ledger.observe(engine="claude", signal=sig)
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is None
    assert ledger.next_reset("claude") == NOW + timedelta(hours=1)
    assert [e["kind"] for e in log.query(kinds=["engine.quota_exhausted"])] == [
        "engine.quota_exhausted"
    ]
    clock.advance(timedelta(hours=1, seconds=1))
    assert ledger.lease(engine_pref=["claude"], purpose="advance") is not None
