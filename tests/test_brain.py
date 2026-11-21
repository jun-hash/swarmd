import pytest
from pydantic import BaseModel

from swarm.brain import EngineBrain
from swarm.engine.base import EngineError
from swarm.engine.fake import FakeEngine
from swarm.inbox.inbox import PolicyMatch
from swarm.ports import BrainError
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.types import UsageSignal


@pytest.fixture
def ledger(db, log, clock):
    return SqliteQuotaLedger(db, log, clock, instance_id="inst_a")


class MessageClass(BaseModel):
    category: str
    confidence: float


@pytest.fixture
def fake_engine():
    return FakeEngine([])


@pytest.fixture
def brain(fake_engine, ledger, log, clock):
    return EngineBrain(fake_engine, ledger, log, clock=clock)


def sig(clock, kind):
    return UsageSignal(kind=kind, reset_at=None, at=clock.now())


def brain_calls(log):
    return [e for e in log.query() if e["kind"] == "brain.call"]


async def test_decide_validates_schema(brain, fake_engine):
    fake_engine.single_shot_returns({"category": "new_work", "confidence": 0.9})
    r = await brain.decide(purpose="classify_message", prompt="...", schema=MessageClass)
    assert r.category == "new_work"
    assert fake_engine.single_shot_calls[0]["json_schema"] == MessageClass.model_json_schema()


async def test_one_retry_then_success(brain, fake_engine):
    fake_engine.single_shot_returns({"nope": 1}, {"category": "x", "confidence": 0.1})
    r = await brain.decide(purpose="report", prompt="P", schema=MessageClass)
    assert r.category == "x"
    assert "P" in fake_engine.single_shot_calls[1]["prompt"]
    assert fake_engine.single_shot_calls[1]["prompt"] != "P"
    assert brain_calls_attempts(fake_engine) == 2


def brain_calls_attempts(fake_engine):
    return len(fake_engine.single_shot_calls)


async def test_two_invalid_outputs_raise_brain_error(brain, fake_engine, log):
    fake_engine.single_shot_returns({"nope": 1}, {"nope": 2})
    with pytest.raises(BrainError):
        await brain.decide(purpose="report", prompt="...", schema=MessageClass)
    (ev,) = brain_calls(log)
    assert ev["payload"]["ok"] is False and ev["payload"]["attempts"] == 2


async def test_cooling_engine_raises_brain_error(brain, ledger, clock, fake_engine):
    ledger.observe(engine="claude", signal=sig(clock, "limit_hit"))
    with pytest.raises(BrainError, match="engine unavailable"):
        await brain.decide(purpose="policy_match", prompt="...", schema=PolicyMatch)
    assert fake_engine.single_shot_calls == []


async def test_engine_limit_signal_observed(brain, ledger, log, clock, fake_engine):
    class Limited(FakeEngine):
        async def single_shot(self, **kw):
            raise EngineError("limit", sig(clock, "limit_hit"))

    b = EngineBrain(Limited([]), ledger, log, clock=clock)
    with pytest.raises(BrainError):
        await b.decide(purpose="report", prompt="x", schema=MessageClass)
    assert ledger.lease(engine_pref=["claude"], purpose="respond") is None


async def test_lease_released_after_call(brain, fake_engine, ledger):
    for _ in range(5):
        fake_engine.single_shot_returns({"category": "a", "confidence": 1})
        await brain.decide(purpose="report", prompt="x", schema=MessageClass)


async def test_every_call_logged(brain, fake_engine, log):
    fake_engine.single_shot_returns({"category": "a", "confidence": 1})
    await brain.decide(purpose="report", prompt="hello", schema=MessageClass)
    (ev,) = brain_calls(log)
    p = ev["payload"]
    assert p["purpose"] == "report" and p["engine"] == "claude"
    assert p["input_chars"] == 5 and p["ok"] is True and p["attempts"] == 1 and "ms" in p
