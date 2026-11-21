"""Fakes for WorkerPool unit tests: harness, browser, guard, ledger, inbox, sleeper."""

import asyncio
from collections.abc import AsyncIterator, Awaitable
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.engine.fake import FakeEngine
from swarm.harness.base import Done, Error, SessionCheckpoint
from swarm.projects.store import SqliteProjectStore
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import (
    AskDraft,
    AskOutcome,
    AskTicket,
    CdpSession,
    Evidence,
    Project,
    QuotaLease,
    UsageSignal,
    Variant,
    WorkOrder,
)
from swarm.workers.pool import WorkerPool
from swarm.workers.verify import Verdict

COMPATIBLE = {("own", "claude", "chrome")}
V_OWN = Variant(harness="own", engine="claude", browser="chrome", context_policy="", model=None)
BLOCK = "block"  # script item: wait until cancelled


def make_project() -> Project:
    return Project(
        id="prj_compet",
        slug="compet",
        name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": []},
        strategy="",
        policies=[],
        autonomy=2,
        memory={"context": "C사 중심", "learnings": [], "failed_attempts": []},
        backlog=[],
        artifacts=[],
        channel="telegram:-100:1",
        board="compet",
        default_profile="work",
        default_variant=V_OWN,
        weight=2,
        watches=[],
    )


def order(**kw) -> WorkOrder:
    o = WorkOrder(
        id="ord_1",
        project_id="prj_compet",
        kind="advance",
        intent="C사 가격 페이지 변화를 확인한다. 이유: 고객보다 먼저 알기. 제약: 읽기만.",
        done_criteria=["가격표 스크린샷이 있다"],
        budget={"max_minutes": 60},
        profile="work",
        priority=1.0,
        value=3,
        due=None,
        backlog_item_id=None,
        action_mode="guarded",
    )
    o.update(kw)  # type: ignore[typeddict-item]
    return o


def evidence(fakes, url="http://c.test/price") -> Evidence:
    return Evidence(url=url, screenshot=None, captured_at=fakes.clock.now(), text="₩9,900")


def add_evidence(fakes, pool):
    def _hook(sink):
        pool.add_evidence(sink.run_id, evidence(fakes))

    return _hook


class FakeHarness:
    id = "own"
    needs_external_browser = True
    needs_engine = True
    capabilities = frozenset({"resume", "steer"})

    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.script: list = [Done(summary="ok")]
        self.scripts: list[list] = []  # per-call scripts (popped first); then `script`
        self.pending_steers: list[str] = []  # reported in the checkpoint
        self.steers: list[tuple[str, str]] = []
        self.checkpoints = True  # record a SessionCheckpoint before each terminal
        self.tasks: list = []
        self.seen_guard: list[str] = []
        self.started = asyncio.Event()
        self.closed = False

    async def run(self, task, *, browser, engine, sink) -> AsyncIterator:
        self.trace.append("harness.start")
        self.tasks.append(task)
        self.seen_guard.append(browser["guard"])
        self.started.set()
        script = self.scripts.pop(0) if self.scripts else self.script
        try:
            for item in script:
                if item == BLOCK:
                    await asyncio.Event().wait()
                if callable(item):
                    r = item(sink)
                    if isinstance(r, Awaitable):
                        await r
                    continue
                if isinstance(item, (Done, Error)) and self.checkpoints:
                    sink.record(
                        SessionCheckpoint(
                            session_id="sess-9",
                            notes=["n"],
                            open_tabs=["http://a/"],
                            pending_steers=list(self.pending_steers),
                        )
                    )
                yield item
        finally:
            self.closed = True

    async def steer(self, handle, text) -> None:
        self.steers.append((handle, text))


class FakeRegistry:
    def __init__(self, harness: FakeHarness) -> None:
        self.harness = harness

    def get(self, harness_id, *, context_policy=None):
        if harness_id != "own":
            raise KeyError(harness_id)
        return self.harness


class FakeBrowser:
    id = "chrome"

    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.profiles = {"work": Path("/tmp/work")}
        self.opened: list[CdpSession] = []
        self.closed: list[str] = []
        self.fail_open = False
        self.open_entered = asyncio.Event()
        self.open_gate: asyncio.Event | None = None  # set -> open_session waits for it
        self.dead: set[str] = set()  # sessions whose browser crashed

    async def open_session(self, *, profile: str, purpose: str) -> CdpSession:
        if self.fail_open:
            raise RuntimeError("browser unavailable")
        self.open_entered.set()
        if self.open_gate is not None:
            await self.open_gate.wait()
        self.trace.append("browser.open")
        s = CdpSession(
            id=f"bs_{len(self.opened) + 1}",
            cdp_url="ws://x",
            target_id="T1",
            profile=profile,
            capabilities=frozenset(),
            guard="off",
        )
        self.opened.append(s)
        return s

    def session_alive(self, session_id: str) -> bool:
        return session_id not in self.dead and session_id not in self.closed

    async def close_session(self, session_id: str) -> None:
        self.trace.append("browser.close")
        self.closed.append(session_id)


class FakeGuard:
    def __init__(self, trace: list[str]) -> None:
        self.trace = trace
        self.connected = True
        self.armed: dict[str, CdpSession] = {}
        self.disarmed: list[str] = []
        self.disarm_entered = asyncio.Event()
        self.disarm_gate: asyncio.Event | None = None  # set -> disarm waits for it
        self.granted: list = []
        self.arm_entered = asyncio.Event()
        self.arm_gate: asyncio.Event | None = None
        self.dedupe_keys: list[str | None] = []

    async def arm(self, session, *, order_id, mode, run_id=None, dedupe_key=None) -> None:
        self.dedupe_keys.append(dedupe_key)
        self.arm_entered.set()
        if self.arm_gate is not None:
            await self.arm_gate.wait()
        self.trace.append(f"guard.arm:{mode}")
        self.armed[session["id"]] = session
        session["guard"] = mode

    async def set_mode(self, session_id, mode) -> None:
        self.trace.append(f"guard.mode:{mode}")
        self.armed[session_id]["guard"] = mode

    def grant(self, token) -> None:
        self.trace.append("guard.grant")
        self.granted.append(token)

    def revoke(self, ask_id) -> bool:
        return False

    async def disarm(self, session_id) -> None:
        self.trace.append("guard.disarm")
        self.disarmed.append(session_id)
        self.disarm_entered.set()
        if self.disarm_gate is not None:
            await self.disarm_gate.wait()
        s = self.armed.pop(session_id, None)
        if s is not None:
            s["guard"] = "off"


class LedgerSpy:
    def __init__(self) -> None:
        self.released: list[QuotaLease] = []
        self.observed: list[tuple[str, UsageSignal]] = []
        self.leased: list[dict] = []
        self.next_lease: QuotaLease | None = None

    def lease(self, *, engine_pref, purpose, max_minutes=60):
        self.leased.append({"engine_pref": list(engine_pref), "purpose": purpose})
        return self.next_lease

    def release(self, lease: QuotaLease) -> None:
        self.released.append(lease)

    def observe(self, *, engine, signal) -> None:
        self.observed.append((engine, signal))


class FakeInbox:
    def __init__(self) -> None:
        self.asks: list[tuple[str, AskDraft]] = []
        self.expired: list[str] = []

    async def raise_ask(self, run_id: str, draft: AskDraft) -> AskTicket:
        self.asks.append((run_id, draft))
        return AskTicket(ask_id=f"ask_{len(self.asks)}", outcome=None)

    def expire_for_run(self, run_id: str) -> list[AskOutcome]:
        self.expired.append(run_id)
        return []


class FakeVerifier:
    """Passes iff the run has evidence (or follows `verdicts` when queued)."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.verdicts: list[Verdict] = []

    async def verify(self, order, artifacts, evidence) -> Verdict:
        self.calls.append((order, artifacts, evidence))
        if self.verdicts:
            return self.verdicts.pop(0)
        if not evidence:
            return Verdict(pass_=False, reasons=["no evidence"], same_engine=True)
        return Verdict(pass_=True, reasons=[], same_engine=True, verifier_run_id="vrf_fake")


class GateSleeper:
    """Records requested sleeps; each sleep returns only once `open()` is called."""

    def __init__(self) -> None:
        self.calls: list[float] = []
        self._gate = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        await self._gate.wait()

    def open(self) -> None:
        self._gate.set()


class Fakes:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.clock = FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))
        self.db = Database(tmp_path / "swarm.db")
        self.log = SqliteEventLog(self.db, BlobStore(tmp_path / "blobs", self.clock))
        self.projects = SqliteProjectStore(self.db, self.log, tmp_path / "home", self.clock)
        self.projects.create(make_project())
        self.trace: list[str] = []
        self.harness = FakeHarness(self.trace)
        self.browser = FakeBrowser(self.trace)
        self.guard = FakeGuard(self.trace)
        self.ledger = LedgerSpy()
        self.inbox = FakeInbox()
        self.engine = FakeEngine([])
        self.sleeper = GateSleeper()
        self.verifier = FakeVerifier()

    async def guards(self, browser_id: str, profile: str) -> FakeGuard:
        return self.guard

    def pool(self) -> WorkerPool:
        return WorkerPool(
            db=self.db,
            projects=self.projects,
            inbox=self.inbox,
            ledger=self.ledger,
            engines={"claude": self.engine},
            compatible=COMPATIBLE,
            browsers={"chrome": self.browser},
            guards=self.guards,
            harnesses=FakeRegistry(self.harness),
            log=self.log,
            clock=self.clock,
            sleeper=self.sleeper,
            verifier=self.verifier,
            output_root=self.tmp_path / "runs",
        )

    def lease(self, minutes: int = 70) -> QuotaLease:
        return QuotaLease(
            id="lse_1",
            engine="claude",
            purpose="advance",
            expires_at=self.clock.now() + timedelta(minutes=minutes),
        )

    def kinds(self, run_id: str) -> list[str]:
        return [e["kind"] for e in self.log.query(run_id=run_id)]


@pytest.fixture
def fakes(tmp_path) -> Fakes:
    return Fakes(tmp_path)


@pytest.fixture
def pool(fakes) -> WorkerPool:
    return fakes.pool()
