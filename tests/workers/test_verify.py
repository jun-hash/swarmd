from datetime import datetime, timedelta

import httpx
import pytest

from swarm.clock import SEOUL
from swarm.engine.base import EngineError
from swarm.engine.fake import FakeEngine
from swarm.types import Artifact, Evidence, QuotaLease
from swarm.workers.verify import VERDICT_SCHEMA, Verifier, VerifierUnavailable

from .conftest import order

NOW = datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL)


class Ledger:
    def __init__(self, leases: list[QuotaLease | None] | None = None) -> None:
        self.asked: list[dict] = []
        self.released: list[QuotaLease] = []
        self.observed: list = []
        self._leases = list(leases) if leases is not None else None

    def lease(self, *, engine_pref, purpose, max_minutes=60):
        self.asked.append({"engine_pref": list(engine_pref), "purpose": purpose})
        if self._leases is None:
            return QuotaLease(
                id=f"lse_v{len(self.asked)}",
                engine="claude",
                purpose=purpose,
                expires_at=NOW + timedelta(minutes=40),
            )
        return self._leases.pop(0) if self._leases else None

    def release(self, lease):
        self.released.append(lease)

    def observe(self, *, engine, signal):
        self.observed.append((engine, signal))


def http(status: int = 200, seen: list | None = None) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(str(request.url))
        return httpx.Response(status, text="ok")

    return httpx.MockTransport(handler)


def ev(url: str | None = "https://c.test/price", screenshot: str | None = None) -> Evidence:
    return Evidence(url=url, screenshot=screenshot, captured_at=NOW, text="₩9,900")


def art(tmp_path) -> Artifact:
    p = tmp_path / "summary.md"
    p.write_text("C사 가격 ₩9,900\n", encoding="utf-8")
    return Artifact(
        id="art_1",
        title="C사 가격",
        path=str(p),
        status="reported",
        evidence=[ev()],
        verifier_run_id=None,
    )


def make(engine, ledger, transport, **kw) -> Verifier:
    return Verifier(engine, ledger, transport=transport, **kw)


async def test_pass_calls_engine_with_schema_under_the_worker_lease(tmp_path):
    engine = FakeEngine([])
    engine.single_shot_returns({"pass": True, "reasons": []})
    ledger = Ledger()
    seen: list[str] = []
    v = make(engine, ledger, http(200, seen))
    verdict = await v.verify(order(), [art(tmp_path)], [ev()])
    assert verdict.pass_ and verdict.same_engine is True and verdict.verifier_run_id
    assert seen == ["https://c.test/price"]
    call = engine.single_shot_calls[0]
    assert call["json_schema"] == VERDICT_SCHEMA
    assert "가격표 스크린샷이 있다" in call["prompt"] and "₩9,900" in call["prompt"]
    assert ledger.asked == [] and ledger.released == []  # runs under the run's own lease


async def test_engine_reject_returns_reasons(tmp_path):
    engine = FakeEngine([])
    engine.single_shot_returns({"pass": False, "reasons": ["가격표가 없다"]})
    v = make(engine, Ledger(), http())
    verdict = await v.verify(order(), [art(tmp_path)], [ev()])
    assert not verdict.pass_ and verdict.reasons == ["가격표가 없다"]


async def test_no_evidence_rejects_without_engine(tmp_path):
    engine = FakeEngine([])
    ledger = Ledger()
    verdict = await make(engine, ledger, http()).verify(order(), [art(tmp_path)], [])
    assert not verdict.pass_ and engine.single_shot_calls == [] and ledger.asked == []


async def test_non_200_url_rejects_without_engine(tmp_path):
    engine = FakeEngine([])
    verdict = await make(engine, Ledger(), http(404)).verify(order(), [art(tmp_path)], [ev()])
    assert not verdict.pass_ and "404" in verdict.reasons[0]
    assert engine.single_shot_calls == []


async def test_non_http_url_rejects(tmp_path):
    engine = FakeEngine([])
    verdict = await make(engine, Ledger(), http()).verify(
        order(), [art(tmp_path)], [ev(url="file:///etc/passwd")]
    )
    assert not verdict.pass_ and engine.single_shot_calls == []


async def test_unreachable_url_rejects(tmp_path):
    def boom(request):
        raise httpx.ConnectError("refused", request=request)

    engine = FakeEngine([])
    verdict = await make(engine, Ledger(), httpx.MockTransport(boom)).verify(
        order(), [art(tmp_path)], [ev()]
    )
    assert not verdict.pass_ and engine.single_shot_calls == []


async def test_missing_screenshot_rejects(tmp_path):
    engine = FakeEngine([])
    missing = str(tmp_path / "nope.png")
    verdict = await make(engine, Ledger(), http()).verify(
        order(), [art(tmp_path)], [ev(url=None, screenshot=missing)]
    )
    assert not verdict.pass_ and "nope.png" in verdict.reasons[0]


async def test_existing_screenshot_only_evidence_passes(tmp_path):
    shot = tmp_path / "s.png"
    shot.write_bytes(b"\x89PNG")
    engine = FakeEngine([])
    engine.single_shot_returns({"pass": True, "reasons": []})
    verdict = await make(engine, Ledger(), http()).verify(
        order(), [art(tmp_path)], [ev(url=None, screenshot=str(shot))]
    )
    assert verdict.pass_


async def test_evidence_with_neither_url_nor_screenshot_rejects(tmp_path):
    engine = FakeEngine([])
    verdict = await make(engine, Ledger(), http()).verify(
        order(), [art(tmp_path)], [ev(url=None, screenshot=None)]
    )
    assert not verdict.pass_


async def test_engine_error_raises_unavailable_and_observes_signal(tmp_path):
    class Boom(FakeEngine):
        async def single_shot(self, **kw):
            raise EngineError("limit", signal={"kind": "limit_hit", "reset_at": None, "at": NOW})

    ledger = Ledger()
    with pytest.raises(VerifierUnavailable):
        await make(Boom([]), ledger, http()).verify(order(), [art(tmp_path)], [ev()])
    assert ledger.observed and ledger.observed[0][0] == "claude"
    assert ledger.asked == [] and ledger.released == []


async def test_malformed_answer_rejects(tmp_path):
    engine = FakeEngine([])
    engine.single_shot_returns({"pass": "yes"})
    verdict = await make(engine, Ledger(), http()).verify(order(), [art(tmp_path)], [ev()])
    assert not verdict.pass_
