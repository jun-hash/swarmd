"""WorkerPool core.

A Run is one WorkOrder with one Variant. `runs.status` is written only by
`RunTable.transition`, which checks the allowed-transition table; finished runs
(done/failed/cancelled) never change again.

Lifecycle of `run()`: validate (raise before any Run exists) -> Run `leased` -> open a browser
session -> arm the ActionGuard (the session reads guard "off" until then) -> `running` ->
consume the harness stream through a RunSink -> terminal:
  Done            -> `verifying` -> Verifier -> `done` | reject: resume once with the reasons,
                     second reject -> `failed` | verifier engine down -> `interrupted`
  Done after Ask  -> `waiting_human`: lease released now, session kept up to 2 h
  Error "quota:*" -> `interrupted` (the QuotaSignal before it went to the ledger)
  Error after Ask -> open Asks expired, then the error path
  other Error     -> retry ladder: resume the same engine session at once, then a fresh engine
                     session after 60 s, then `failed` + project failure memory
The lease is released on every exit path; a non-parked exit disarms the guard, then closes
the session. A session whose browser died (Chrome crash: `session_alive` False or the guard
disconnected) is dropped before the next turn or resume and the profile reopened.

Resume (`resume`, `release`, verify reject, retry) re-enters the same driver with a
`HarnessTask.resume` built from the last SessionCheckpoint. The one human-resume edge is
waiting_human -> running (interrupted -> running for a quota stop that left an Ask open). Lines
for the worker that are not yet delivered (steers while parked, the rendered answer, checkpoint
`pending_steers`) live in `runs.pending_json` until a `Done` shows the harness got them.
Takeover reuses waiting_human with `taken_over=1` (guard "off", no 2 h close, Ask answers wait).
"""

import asyncio
import contextlib
import dataclasses
import functools
import json
import logging
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol

from swarm.clock import Clock
from swarm.engine.base import QUOTA_ERROR_PREFIX, Engine
from swarm.harness.base import (
    AskRaised,
    Done,
    Error,
    FileOutput,
    HarnessAdapter,
    HarnessEvent,
    NotSupported,
    QuotaSignal,
    SessionCheckpoint,
    SinkRecord,
    Thinking,
    ToolCall,
    ToolResult,
    TurnUsage,
    UnknownHandle,
)
from swarm.ids import new_id
from swarm.inbox.inbox import Inbox
from swarm.ports import ApprovalToken, BrowserRuntime, Guard, RequestDescriptor
from swarm.projects.store import ProjectStore
from swarm.scheduler.ledger import QuotaLedger
from swarm.store.codec import to_json
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.text import clip
from swarm.types import (
    Artifact,
    AskDraft,
    AskOutcome,
    AskTicket,
    BrowserId,
    CdpSession,
    Cost,
    EngineId,
    Event,
    Evidence,
    FailedAttempt,
    HarnessId,
    HarnessResume,
    HarnessTask,
    ProjectId,
    QuotaLease,
    RunId,
    RunResult,
    SteerMode,
    TakeoverInfo,
    Variant,
    WorkerRun,
    WorkOrder,
)
from swarm.workers.approval import (
    APPROVAL_KINDS,
    bound_request,
    harden_approval_draft,
    is_approving_answer,
)
from swarm.workers.contract import (
    FINISHED,
    IllegalTransition,
    IncompatibleVariant,
    InvalidOrder,
    LeaseExpired,
    RunFinished,
    UnknownProfile,
    cancelled_summary,
    dedupe_key,
    order_from_json,
    paused_summary,
    render_outcome,
    steer_line,
    validate_order,
)
from swarm.workers.live import LiveRun, NextTurn, PoolSink
from swarm.workers.runs import (
    ACTIVE,
    RunTable,
    evidence_to_dict,
    result_from_json,
    result_to_json,
)
from swarm.workers.verify import Verdict, Verifier, VerifierUnavailable, collect_artifacts

log = logging.getLogger(__name__)

PARK_SESSION_MAX = timedelta(hours=2)
MEMORY_BRIEF_TOKENS = 2_000
RECENT_FAILURES = 5
SUMMARY_MAX = 600
RETRY_FRESH_AFTER_S = 60  # second engine-error retry: fresh engine session after this wait
VERIFY_REJECTS_MAX = 2  # first reject -> resume once with the reasons; second -> failed

_EVENT_KINDS: dict[type, str] = {
    Thinking: "worker.thinking",
    ToolCall: "worker.tool_call",
    ToolResult: "worker.tool_result",
    AskRaised: "worker.ask_raised",
    FileOutput: "worker.file_output",
    TurnUsage: "worker.usage",
    QuotaSignal: "worker.quota_signal",
    Done: "worker.done",
    Error: "worker.error",
}


class HarnessSource(Protocol):
    def get(
        self, harness_id: HarnessId, *, context_policy: str | None = None
    ) -> HarnessAdapter: ...


# GuardProvider(browser_id, profile) -> the ONE ActionGuard of that profile's browser. The
# provider must return the same guard for every call with the same (browser, profile) while
# that guard's CDP connection lives: arms, approval tokens and the browser-level Fetch epoch
# live on that guard, and `ChromeRuntime.open_guard` opens a new CDP connection per call, so
# the daemon wires a per-profile cache around it. A disconnected guard (browser crash) has
# nothing worth keeping; the cache replaces it with a fresh one.
GuardProvider = Callable[[BrowserId, str], Awaitable[Guard]]
Sleeper = Callable[[float], Awaitable[None]]


class VerifierLike(Protocol):
    async def verify(
        self, order: WorkOrder, artifacts: list[Artifact], evidence: list[Evidence]
    ) -> Verdict: ...


def _payload(ev: object) -> dict:
    return dataclasses.asdict(ev) if dataclasses.is_dataclass(ev) else {"value": str(ev)}


class WorkerPool:
    def __init__(
        self,
        *,
        db: Database,
        projects: ProjectStore,
        inbox: Inbox,
        ledger: QuotaLedger,
        engines: Mapping[str, Engine],
        compatible: Collection[tuple[HarnessId, EngineId, BrowserId]],
        browsers: Mapping[BrowserId, BrowserRuntime],
        guards: GuardProvider,
        harnesses: HarnessSource,
        log: EventLog,
        clock: Clock,
        output_root: Path,
        sleeper: Sleeper = asyncio.sleep,
        verifier: VerifierLike | None = None,
    ) -> None:
        self._db = db
        self._projects = projects
        self._inbox = inbox
        self._ledger = ledger
        self._engines = dict(engines)
        self._compatible = frozenset(compatible)
        self._browsers = dict(browsers)
        self._guards = guards
        self._harnesses = harnesses
        self._log = log
        self._clock = clock
        self._emit = Emitter(log, clock, "workers")
        self._sleep = sleeper
        self._output_root = output_root
        self._live: dict[RunId, LiveRun] = {}
        # resume / takeover / release / cancel of one run never interleave: each holds the
        # run's lock over its whole state-changing section (until a driver is launched or
        # the change is fully applied); the driver itself never takes it
        self._locks: dict[RunId, asyncio.Lock] = {}
        self._lock_users: dict[RunId, int] = {}  # holders + waiters (for pruning)
        self._verifier = verifier
        self._verifiers: dict[str, Verifier] = {}
        self._runs = RunTable(db, self._emit, clock)

    # --- queries -------------------------------------------------------------------
    # Inside the pool every read and write of the table goes through `self.runs`. The
    # one-line delegations below are the pool's public read API, for its callers only.

    @property
    def runs(self) -> RunTable:
        """The runs table (read-only attribute; the pool is its only writer)."""
        return self._runs

    def get(self, run_id: RunId) -> WorkerRun:
        return self.runs.get(run_id)

    def running(self) -> list[WorkerRun]:
        return self.runs.active()

    def run_project(self, run_id: RunId) -> ProjectId | None:
        return self.runs.project(run_id)

    def order(self, run_id: RunId) -> WorkOrder | None:
        """The WorkOrder a run executes (None for an unknown run)."""
        return self.runs.order(run_id)

    def result(self, run_id: RunId) -> RunResult | None:
        """The stored RunResult of the run's last finished drive (None before one)."""
        return self.runs.result(run_id)

    def checkpoint(self, run_id: RunId) -> dict | None:
        """Latest SessionCheckpoint of the run (raw material for HarnessTask.resume)."""
        return self.runs.checkpoint(run_id)

    def evidence(self, run_id: RunId) -> list[Evidence]:
        return self.runs.evidence(run_id)

    def session(self, run_id: RunId) -> CdpSession | None:
        """The live browser session (None when closed or being torn down)."""
        rec = self._live.get(run_id)
        if rec is None or rec.tearing_down is not None:
            return None
        return rec.session

    def recover_on_boot(self) -> list[RunId]:
        """running / leased / verifying Runs of a dead instance -> interrupted."""
        return self.runs.interrupt_orphans()

    def _is_finished(self, run_id: RunId) -> bool:
        r = self.runs.row(run_id)
        return r is None or r["status"] in FINISHED

    def _unfinished(self, run_id: RunId):
        r = self.runs.row(run_id)
        if r is None:
            raise KeyError(run_id)
        if r["status"] in FINISHED:
            raise RunFinished(f"run {run_id} is {r['status']}")
        return r

    def _row_result(self, r) -> RunResult:
        """The last RunResult the run returned (finished / parked); else built from the row."""
        if r["result_json"] and r["status"] in (*FINISHED, "waiting_human", "interrupted"):
            return result_from_json(r["result_json"])
        end = datetime.fromisoformat(r["ended_at"]) if r["ended_at"] else self._clock.now()
        minutes = (end - datetime.fromisoformat(r["started_at"])).total_seconds() / 60
        return RunResult(
            run_id=r["id"],
            # RunResult statuses are frozen: an active row without a live driver (e.g. after a
            # restart before recover_on_boot) reads as interrupted
            status="interrupted" if r["status"] in ACTIVE - {"waiting_human"} else r["status"],
            artifacts=[],
            summary=r["summary"],
            cost=Cost(turns=0, minutes=round(minutes, 3), engine_usage={}),
            failure=json.loads(r["failure_json"]) if r["failure_json"] else None,
        )

    # --- run -----------------------------------------------------------------------

    def _check(
        self,
        order: WorkOrder,
        variant: Variant,
        lease: QuotaLease | None,
        *,
        require_lease: bool = True,
    ):
        validate_order(order)
        if order["project_id"] is not None:
            try:
                self._projects.get(order["project_id"])
            except KeyError as e:
                raise InvalidOrder(f"unknown project {order['project_id']}") from e
        engine_id = variant.get("engine") or (lease["engine"] if lease else None)
        key = (variant.get("harness"), engine_id, variant.get("browser"))
        if key not in self._compatible:
            raise IncompatibleVariant(f"variant {key} not in {sorted(self._compatible)}")
        browser = self._browsers.get(variant["browser"])
        if browser is None:
            raise IncompatibleVariant(f"browser {variant['browser']} not available")
        if order["profile"] not in browser.profiles:
            raise UnknownProfile(order["profile"])
        try:
            adapter = self._harnesses.get(
                variant["harness"], context_policy=variant.get("context_policy") or None
            )
        except KeyError as e:
            raise IncompatibleVariant(f"harness {variant['harness']} not registered") from e
        engine: Engine | None = None
        if adapter.needs_engine and (lease is not None or require_lease):
            if lease is None:
                raise LeaseExpired("harness needs an engine: lease required")
            if lease["expires_at"] <= self._clock.now():
                raise LeaseExpired(f"lease {lease['id']} expired at {lease['expires_at']}")
            if lease["engine"] != engine_id or engine_id not in self._engines:
                raise IncompatibleVariant(f"lease engine {lease['engine']} != {engine_id}")
            engine = self._engines[engine_id]
        return adapter, engine, engine_id, browser

    async def run(
        self, order: WorkOrder, variant: Variant, *, lease: QuotaLease | None
    ) -> RunResult:
        adapter, engine, engine_id, browser = self._check(order, variant, lease)
        run_id = new_id("run")
        now = self._clock.now()
        out_dir = self._output_root / run_id
        self.runs.insert(run_id, order, variant, started=now)
        rec = LiveRun(
            run_id=run_id,
            order=order,
            variant=variant,
            adapter=adapter,
            engine=engine,
            browser=browser,
            output_dir=out_dir,
            started=now,
            lease=lease,
            engine_id=engine_id,
        )
        self._live[run_id] = rec
        self.runs.transition(run_id, "leased")
        try:
            rec.driver = self._spawn(rec, self._build_task(rec.order))
            return await rec.driver
        finally:
            self._release_lease(rec)

    def _spawn(self, rec: LiveRun, task: HarnessTask) -> asyncio.Task:
        """Start the driver eagerly: it is inside its own try/except before anyone else can
        cancel it (a task cancelled before its first step would skip all cleanup)."""
        loop = asyncio.get_running_loop()
        return asyncio.Task(self._drive(rec, task), loop=loop, eager_start=True)

    def _release_lease(self, rec: LiveRun) -> None:
        lease, rec.lease = rec.lease, None
        if lease is not None:
            try:
                self._ledger.release(lease)
            except Exception:
                log.warning("lease release failed for %s", rec.run_id, exc_info=True)

    def _build_task(self, order: WorkOrder) -> HarnessTask:
        brief, policies, failures = "", [], []
        pid = order["project_id"]
        if pid:
            brief = self._projects.memory_brief(pid, budget_tokens=MEMORY_BRIEF_TOKENS)["text"]
            p = self._projects.get(pid)
            policies = [x["text"] for x in p["policies"] if x["active"]]
            failures = [
                f"{f['what']} -> {f['why']}"
                for f in p["memory"]["failed_attempts"][-RECENT_FAILURES:]
            ]
        return HarnessTask(
            instruction=order["intent"],
            done_criteria=list(order["done_criteria"]),
            start_url=None,
            memory_brief=brief,
            policies=policies,
            budget=order["budget"],
            action_mode=order["action_mode"],
            recent_failures=failures,
        )

    # --- drive ---------------------------------------------------------------------

    async def _drive(self, rec: LiveRun, task: HarnessTask) -> RunResult:
        # Started eagerly (`_spawn`): until the first real suspension this runs inside
        # `_spawn` itself, so `rec.driver` is still unset (or the previous, finished task).
        # Nothing on that path may rely on `rec.driver`.
        try:
            if rec.session is None or not rec.armed:
                try:
                    await self._open_session(rec)
                except Exception as e:  # noqa: BLE001 - browser trouble interrupts, never raises
                    log.warning("run %s: browser setup failed", rec.run_id, exc_info=True)
                    return await self._finish(rec, "interrupted", f"browser unavailable: {e}")
            resumed = "resume" in task
            self.runs.transition(rec.run_id, "running")
            self._emit(
                "run.resumed" if resumed else "run.started",
                {"order_id": rec.order["id"], "variant": rec.variant, "session": rec.session["id"]},
                run_id=rec.run_id,
                project_id=rec.order["project_id"],
            )
            while True:
                if await self._drop_dead_session(rec):  # Chrome died: reopen
                    try:
                        await self._open_session(rec)
                    except Exception as e:  # noqa: BLE001
                        log.warning("run %s: browser reopen failed", rec.run_id, exc_info=True)
                        return await self._finish(rec, "interrupted", f"browser unavailable: {e}")
                    task = self._announce_reopen(rec, task)
                terminal = await self._consume(rec, task)
                nxt = await self._on_terminal(rec, terminal)
                if not isinstance(nxt, NextTurn):
                    return nxt
                task = nxt.task
        except asyncio.CancelledError:
            if rec.cancel_reason is None and rec.taking_over:
                return await self._park_taken_over(rec)
            if rec.cancel_reason is None:  # the caller went away (shutdown): resumable later
                with contextlib.suppress(Exception):
                    if not self._is_finished(rec.run_id):
                        await self._finish(rec, "interrupted", "run task cancelled")
                raise
            return await self._finish_cancelled(rec)
        except Exception as e:  # noqa: BLE001 - a pool bug must not leak the session or lease
            log.exception("run %s: internal error", rec.run_id)
            return await self._crash(rec, e)

    def _session_dead(self, rec: LiveRun) -> bool:
        if rec.session is None or rec.tearing_down is not None:
            return False
        if rec.guard is not None and not rec.guard.connected:
            return True  # its guard lost Chrome: the tab is closed or no longer intercepted
        return not rec.browser.session_alive(rec.session["id"])

    async def _drop_dead_session(self, rec: LiveRun) -> bool:
        """The browser crashed under the run: forget the dead session and guard, so the next
        `_open_session` opens a fresh session on the (relaunched) profile and arms the
        GuardProvider's fresh guard. True if one was dropped."""
        if not self._session_dead(rec):
            return False
        session = rec.session
        assert session is not None
        rec.session, rec.armed, rec.guard = None, False, None
        with contextlib.suppress(Exception):
            await rec.browser.close_session(session["id"])  # forget it (never raises)
        self._emit(
            "run.browser_lost",
            {"session": session["id"]},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        return True

    def _reopen_line(self, rec: LiveRun) -> str:
        tabs = (self.runs.checkpoint(rec.run_id) or {}).get("open_tabs") or []
        return "브라우저가 다시 열림, 마지막 URL 목록: " + (", ".join(tabs) or "(없음)")

    def _announce_reopen(self, rec: LiveRun, task: HarnessTask) -> HarnessTask:
        """The next turn learns its browser was reopened (the resume text lists the URLs)."""
        if "resume" not in task:
            return task
        res = HarnessResume(**task["resume"])
        res["message"] = f"{self._reopen_line(rec)}\n{res['message']}"
        return HarnessTask(**{**task, "resume": res})

    async def _open_session(self, rec: LiveRun) -> None:
        if rec.session is None:
            rec.armed = False
            rec.session = await rec.browser.open_session(
                profile=rec.order["profile"], purpose=rec.order["kind"]
            )
        await self._arm(rec)

    async def _arm(self, rec: LiveRun) -> None:
        assert rec.session is not None
        rec.guard = await self._guards(rec.variant["browser"], rec.order["profile"])
        # the harness must never see an unguarded session (guard "off" until armed)
        await rec.guard.arm(
            rec.session,
            order_id=rec.order["id"],
            mode=rec.order["action_mode"],
            run_id=rec.run_id,
            dedupe_key=dedupe_key(rec.order),
        )
        rec.armed = True

    async def _crash(self, rec: LiveRun, e: Exception) -> RunResult:
        """Internal error anywhere in the run: failed + failure memory, always torn down."""
        why = f"internal error: {type(e).__name__}: {e}"
        r = self.runs.row(rec.run_id)
        if r is not None and r["status"] in FINISHED:  # failed after the final transition
            await self._teardown(rec)
            self._release_lease(rec)
            self._live.pop(rec.run_id, None)
            return self._result(rec, r["status"], r["summary"] or why)
        if rec.asked:
            with contextlib.suppress(Exception):
                self._inbox.expire_for_run(rec.run_id)
        try:
            return await self._fail(rec, why)
        except Exception:
            log.exception("run %s: could not record the failure", rec.run_id)
            await self._teardown(rec)
            self._release_lease(rec)
            return self._result(rec, "failed", why)

    async def _consume(self, rec: LiveRun, task: HarnessTask) -> Done | Error:
        rec.segment_started = self._clock.now()
        rec.segment_checkpoints = rec.checkpoints
        rec.output_dir.mkdir(parents=True, exist_ok=True)
        sink = PoolSink(
            rec.run_id,
            rec.output_dir,
            functools.partial(self.raise_ask, rec.run_id),
            functools.partial(self._on_record, rec),
        )
        terminal: Done | Error | None = None
        stream = rec.adapter.run(task, browser=rec.session, engine=rec.engine, sink=sink)
        async with contextlib.aclosing(stream) as events:
            async for ev in events:
                if terminal is not None:
                    continue  # contract: one terminal; ignore anything after it
                self._on_event(rec, ev)
                if isinstance(ev, (Done, Error)):
                    terminal = ev
        terminal = terminal or Error(message="harness ended without a terminal event")
        self._settle_pending(rec, delivered=isinstance(terminal, Done))
        return terminal

    def _on_event(self, rec: LiveRun, ev: HarnessEvent) -> None:
        self._emit(
            _EVENT_KINDS[type(ev)],
            _payload(ev),
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        if isinstance(ev, QuotaSignal) and rec.engine_id is not None:
            self._ledger.observe(engine=rec.engine_id, signal=ev.signal)
        elif isinstance(ev, AskRaised):
            rec.asked = True
        elif isinstance(ev, FileOutput):
            if all(f.path != ev.path for f in rec.files):  # same file again in a later turn
                rec.files.append(ev)
        elif isinstance(ev, TurnUsage):
            rec.turns += 1
            rec.usage["input_tokens"] += ev.input_tokens
            rec.usage["output_tokens"] += ev.output_tokens
            rec.usage["cached_tokens"] += ev.cached_tokens
            rec.usage["cost_usd"] += ev.cost_usd

    def _on_record(self, rec: LiveRun, ev: SinkRecord) -> None:
        kind = getattr(ev, "kind", None) or _EVENT_KINDS.get(type(ev), "worker.record")
        payload = _payload(ev)
        self._emit(kind, payload, run_id=rec.run_id, project_id=rec.order["project_id"])
        if isinstance(ev, SessionCheckpoint):
            rec.checkpoints += 1
            with contextlib.suppress(RunFinished):
                self.runs.update(rec.run_id, checkpoint_json=to_json(payload))

    def add_evidence(self, run_id: RunId, ev: Evidence) -> None:
        """Evidence posted by the worker (internal API `runs/{id}/evidence`)."""
        with self._db.transaction():
            r = self.runs.row(run_id)
            if r is None:
                raise KeyError(run_id)
            items = json.loads(r["evidence_json"])
            items.append(evidence_to_dict(ev))
            self.runs.update(run_id, evidence_json=json.dumps(items, ensure_ascii=False))
            self._emit(
                "run.evidence", evidence_to_dict(ev), run_id=run_id, project_id=r["project_id"]
            )

    def _settle_pending(self, rec: LiveRun, *, delivered: bool) -> None:
        """After a harness turn. A Done shows this turn's message got through: exactly the
        pending lines it carried are dropped; lines appended meanwhile stay. Unapplied steers
        of a checkpoint recorded in THIS turn are added; an older (stale) checkpoint is
        ignored, its steers were already handled."""
        steers: list[str] = []
        if rec.checkpoints > rec.segment_checkpoints:
            cp = self.runs.checkpoint(rec.run_id) or {}
            steers = [steer_line(t) for t in cp.get("pending_steers") or []]
        cur = self.runs.pending(rec.run_id)
        if delivered:
            for line in rec.delivering:
                if line in cur:
                    cur.remove(line)
            rec.delivering = []
        parts = cur + [s for s in steers if s not in cur]
        with contextlib.suppress(RunFinished):
            self.runs.set_pending(rec.run_id, parts)

    # --- terminal ------------------------------------------------------------------

    async def _on_terminal(self, rec: LiveRun, terminal: Done | Error) -> RunResult | NextTurn:
        if isinstance(terminal, Error):
            if terminal.message.startswith(QUOTA_ERROR_PREFIX):
                return await self._finish(rec, "interrupted", paused_summary(terminal.message))
            expired = rec.asked
            if rec.asked:  # nobody will resume this turn: close its Asks
                self._inbox.expire_for_run(rec.run_id)
                rec.asked = False
            return await self._retry(rec, terminal.message, asks_expired=expired)
        if rec.asked:
            return await self._park(rec, terminal.summary)
        self.runs.transition(rec.run_id, "verifying")
        evidence = self.runs.evidence(rec.run_id)  # one snapshot for artifacts and verdict
        artifacts = collect_artifacts(
            rec.output_dir, rec.order["intent"], rec.files, evidence, terminal.summary
        )
        try:
            verdict = await self._verify(rec, artifacts, evidence)
        except VerifierUnavailable as e:
            return await self._finish(rec, "interrupted", f"verifier unavailable: {e}")
        if verdict.pass_ and (not evidence or not verdict.verifier_run_id):
            # invariant: verified => evidence >= 1 and a verifier run id
            verdict = Verdict(
                pass_=False,
                reasons=["검증 통과에 증거와 verifier_run_id가 필요함"],
                same_engine=verdict.same_engine,
                verifier_run_id=verdict.verifier_run_id,
            )
        if not verdict.pass_:
            rejects = self.runs.bump(rec.run_id, "verify_rejects")
            reasons = "; ".join(verdict.reasons) or "완료 기준 미충족"
            if rejects >= VERIFY_REJECTS_MAX:
                return await self._fail(rec, f"verify rejected: {reasons}", artifacts)
            self.runs.transition(rec.run_id, "running")
            return NextTurn(self._resume_task(rec, f"검증 거절: {reasons}"))
        verified = [
            {
                **a,
                "status": "verified",
                "verifier_run_id": verdict.verifier_run_id,
                "verifier_same_engine": verdict.same_engine,
            }
            for a in artifacts
        ]
        return await self._finish(rec, "done", terminal.summary, artifacts=verified)

    async def _retry(
        self, rec: LiveRun, message: str, *, asks_expired: bool
    ) -> RunResult | NextTurn:
        """Engine error ladder: same session now -> fresh session after 60 s -> failed."""
        retries = self.runs.bump(rec.run_id, "retries")
        if retries > 2:
            return await self._fail(rec, message)
        attempt = self.runs.bump(rec.run_id, "attempts")  # monotonic over the run's life
        line = f"엔진 오류로 끊겼음 ({clip(message, 200)}). 하던 일을 이어서 진행해."
        if asks_expired:
            line += " 보낸 질문은 취소됨: 필요하면 다시 물어봐."
        fresh = retries == 2
        if fresh:
            await self._sleep(RETRY_FRESH_AFTER_S)
        self._emit(
            "run.retry",
            {"attempt": attempt, "fresh_session": fresh, "error": clip(message, 500)},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        return NextTurn(self._resume_task(rec, line, fresh=fresh))

    def _verifier_for(self, rec: LiveRun) -> VerifierLike:
        if self._verifier is not None:
            return self._verifier
        engine_id = rec.engine_id
        assert engine_id is not None  # `_check` admits only variants with an engine
        if engine_id not in self._verifiers:
            self._verifiers[engine_id] = Verifier(
                self._engines[engine_id], self._ledger, engine_id=engine_id
            )
        return self._verifiers[engine_id]

    async def _verify(
        self, rec: LiveRun, artifacts: list[Artifact], evidence: list[Evidence]
    ) -> Verdict:
        return await self._verifier_for(rec).verify(rec.order, artifacts, evidence)

    async def _teardown(self, rec: LiveRun) -> None:
        """Disarm the guard, then close the session. Never raises (except the caller's own
        cancellation); the work runs shielded, so a cancelled caller cannot skip the close.
        `rec.tearing_down` marks it in flight; concurrent callers wait for the same task."""
        if rec.tearing_down is None:
            if rec.session is None:
                return
            rec.tearing_down = asyncio.ensure_future(self._close_session(rec, rec.session))
        await asyncio.shield(rec.tearing_down)

    async def _close_session(self, rec: LiveRun, session: CdpSession) -> None:
        try:
            if rec.guard is not None:
                try:
                    await rec.guard.disarm(session["id"])
                except Exception:
                    log.warning("disarm failed for %s", rec.run_id, exc_info=True)
            try:
                await rec.browser.close_session(session["id"])
            except Exception:
                log.warning("close_session failed for %s", rec.run_id, exc_info=True)
        finally:
            if rec.session is session:
                rec.session = None
                rec.armed = False
            rec.tearing_down = None

    def _cost(self, rec: LiveRun) -> Cost:
        minutes = (self._clock.now() - rec.started).total_seconds() / 60
        return Cost(turns=rec.turns, minutes=round(minutes, 3), engine_usage=dict(rec.usage))

    def _result(
        self,
        rec: LiveRun,
        status: str,
        summary: str,
        *,
        artifacts: list[Artifact] | None = None,
        failure: FailedAttempt | None = None,
    ) -> RunResult:
        return RunResult(
            run_id=rec.run_id,
            status=status,
            artifacts=list(artifacts or []),
            summary=clip(summary, SUMMARY_MAX),
            cost=self._cost(rec),
            failure=failure,
        )

    async def _finish(
        self,
        rec: LiveRun,
        status: str,
        summary: str,
        *,
        artifacts: list[Artifact] | None = None,
        failure: FailedAttempt | None = None,
    ) -> RunResult:
        if rec.park_timer is not None and rec.park_timer is not asyncio.current_task():
            rec.park_timer.cancel()
        self._revoke_grants(rec, status)  # every terminal status, before anything else
        await self._teardown(rec)
        self._release_lease(rec)
        result = self._result(rec, status, summary, artifacts=artifacts, failure=failure)
        if status in ("done", "failed"):
            dropped = self.runs.pending(rec.run_id)
            if dropped:  # queued for a resume that will never come: the human should know
                self._emit(
                    "run.pending_dropped",
                    {"lines": dropped},
                    run_id=rec.run_id,
                    project_id=rec.order["project_id"],
                )
        self.runs.update(
            rec.run_id,
            summary=clip(summary, SUMMARY_MAX),
            failure_json=to_json(failure) if failure else None,
            result_json=result_to_json(result),
        )
        if not (status == "interrupted" and self.runs.row(rec.run_id)["status"] == status):
            self.runs.transition(rec.run_id, status)  # (a resume that failed again stays put)
        self._emit(
            "run.finished",
            {"status": status},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        if status in FINISHED:
            self._live.pop(rec.run_id, None)
        self._prune_lock(rec.run_id)  # the drive ended outside `_control`: nobody else prunes
        return result

    async def _fail(
        self, rec: LiveRun, why: str, artifacts: list[Artifact] | None = None
    ) -> RunResult:
        failure = FailedAttempt(what=clip(rec.order["intent"], 200), why=why, run_id=rec.run_id)
        if rec.order["project_id"]:
            try:
                self._projects.record_failure(rec.order["project_id"], failure)
            except Exception:
                log.warning("record_failure failed for %s", rec.run_id, exc_info=True)
        partial = [{**a, "status": "partial", "verifier_run_id": None} for a in artifacts or []]
        return await self._finish(
            rec, "failed", f"failed: {why}", artifacts=partial, failure=failure
        )

    async def _park(self, rec: LiveRun, summary: str) -> RunResult:
        """waiting_human: lease back now, session kept (closed after PARK_SESSION_MAX)."""
        now = self._clock.now()
        result = self._result(rec, "waiting_human", summary)
        self.runs.update(
            rec.run_id,
            parked_at=now.isoformat(),
            summary=clip(summary, SUMMARY_MAX),
            result_json=result_to_json(result),
        )
        self.runs.transition(rec.run_id, "waiting_human")
        self._release_lease(rec)
        self._emit(
            "run.parked",
            {
                "session": rec.session["id"] if rec.session else None,
                "close_after": (now + PARK_SESSION_MAX).isoformat(),
            },
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        rec.park_timer = asyncio.create_task(self._close_parked_session(rec))
        return result

    async def _close_parked_session(self, rec: LiveRun) -> None:
        await self._sleep(PARK_SESSION_MAX.total_seconds())
        r = self.runs.row(rec.run_id)
        if (
            r is None
            or r["status"] != "waiting_human"
            or rec.session is None
            or rec.tearing_down is not None
        ):
            return  # resumed / cancelled / already being closed meanwhile
        sid = rec.session["id"]
        await self._teardown(rec)
        self._emit(
            "run.session_closed",
            {"session": sid},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )

    async def _finish_cancelled(self, rec: LiveRun) -> RunResult:
        self._inbox.expire_for_run(rec.run_id)
        return await self._finish(rec, "cancelled", cancelled_summary(rec.cancel_reason))

    # --- asks ----------------------------------------------------------------------

    async def raise_ask(self, run_id: RunId, draft: AskDraft) -> AskTicket:
        """Every worker Ask goes through here: the harness sink, and the internal API
        `runs/{id}/asks` (like evidence through `add_evidence`). Approval / permission Asks
        are always irreversible (no default), and are bound to the request the guard blocked
        for this order during the current harness turn, i.e. the one the human is shown."""
        rec = self._live.get(run_id)
        descriptor: RequestDescriptor | None = None
        draft, forced = harden_approval_draft(draft)
        if draft["kind"] in APPROVAL_KINDS:
            descriptor = self._blocked_in_segment(rec) if rec is not None else None
        if forced:
            self._emit(
                "ask.irreversible_forced",
                {"kind": draft["kind"], "forced": forced},
                run_id=run_id,
                project_id=self.runs.project(run_id),
            )
        ticket = await self._inbox.raise_ask(run_id, draft)
        self.runs.note_ask(
            run_id,
            ticket["ask_id"],
            {
                "kind": draft["kind"],
                "options": list(draft.get("options") or []),
                "recommended": draft.get("recommended"),
                "descriptor": descriptor,
            },
        )
        return ticket

    def _blocked_in_segment(self, rec: LiveRun) -> RequestDescriptor | None:
        """The request an approval Ask is about: bound only when exactly ONE distinct
        (method, host, path) of this run's order was blocked as `needs_token` in the current
        harness turn. Zero or several -> nothing is bound (`ask.approval_unbound`), so no
        token can be granted and the worker has to ask again."""
        blocked: Iterable[Event] = ()
        if rec.segment_started is not None:
            blocked = self._log.query(
                run_id=rec.run_id, kinds=["guard.blocked"], since=rec.segment_started
            )
        bound, candidates = bound_request(blocked, rec.order["id"])
        if bound is not None:
            return bound
        self._emit(
            "ask.approval_unbound",
            {"candidates": candidates},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        return None

    async def _grant_approval(self, rec: LiveRun, outcome: AskOutcome) -> ApprovalToken | None:
        """A human's approving answer to an approval/permission Ask lets exactly the
        request bound to that Ask (captured when it was raised) through once. Never on
        defaults, policy answers, free text or a choice outside the Ask's own options."""
        meta = self.runs.ask_meta(rec.run_id, outcome["ask_id"])
        if not is_approving_answer(meta, outcome):
            return None
        d = meta.get("descriptor")
        if d is None:
            self._emit(
                "run.approval_unused",
                {"ask_id": outcome["ask_id"], "why": "no blocked request bound to the ask"},
                run_id=rec.run_id,
                project_id=rec.order["project_id"],
            )
            return None
        if rec.guard is None or not rec.guard.connected:
            rec.guard = await self._guards(rec.variant["browser"], rec.order["profile"])
        token = ApprovalToken.from_descriptor(outcome["ask_id"], d, order_id=rec.order["id"])
        try:
            rec.guard.grant(token)
        except ValueError as e:  # one Ask = one token: already granted / already executed
            log.warning("run %s: approval not granted: %s", rec.run_id, e)
            return None
        rec.granted.append(token.ask_id)  # tracked first: a failing emit must not orphan it
        self._emit(
            "run.approval_granted",
            {
                "ask_id": token.ask_id,
                "method": token.method,
                "host": token.host,
                "path_pattern": token.path_pattern,
            },
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        return token

    def _revoke(self, rec: LiveRun, ask_id: str, why: str) -> bool:
        """`guard.revoke(ask_id)`; `guard.grant_revoked` only when a token was really
        removed. Raises if the guard's revoke raises."""
        assert rec.guard is not None
        if rec.guard.revoke(ask_id):
            self._emit(
                "guard.grant_revoked",
                {"ask_id": ask_id, "why": why},
                run_id=rec.run_id,
                project_id=rec.order["project_id"],
            )
            return True
        return False

    def _revoke_grants(self, rec: LiveRun, why: str) -> None:
        """The run ends: no token it was granted may outlive it (the guard is per profile
        and long-lived; a later run of the same order must ask again)."""
        granted, rec.granted = rec.granted, []
        if not granted or rec.guard is None:
            return
        for ask_id in granted:
            try:
                self._revoke(rec, ask_id, why)
            except Exception:
                log.warning("run %s: revoke of %s failed", rec.run_id, ask_id, exc_info=True)

    def _drop_grant(self, rec: LiveRun, token: ApprovalToken) -> None:
        """A resume failed after its grant: revoke the token, or log it if the revoke fails
        (it still matches only that one request of this order, once)."""
        if rec.guard is not None:
            try:
                self._revoke(rec, token.ask_id, "resume failed")
                if token.ask_id in rec.granted:
                    rec.granted.remove(token.ask_id)
                return
            except Exception:
                log.warning("run %s: revoke failed", rec.run_id, exc_info=True)
        self._emit(
            "guard.grant_orphaned",
            {"ask_id": token.ask_id, "method": token.method, "host": token.host},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )

    # --- resume --------------------------------------------------------------------

    async def resume(
        self, run_id: RunId, *, outcome: AskOutcome, lease: QuotaLease | None
    ) -> RunResult:
        """Continue a parked (or quota-interrupted) run with the human's answer. Finished
        runs and `expired` outcomes return the current result without running anything. An
        answer while a human holds the browser is kept for `release`; while a drive is live
        it is queued and the result of that drive is returned."""
        launched = False
        follow = False  # a drive is live: our line is queued, wait for its result
        try:
            async with self._control(run_id):
                r = self.runs.row(run_id)
                if r is None:
                    raise KeyError(run_id)
                if r["status"] in FINISHED or outcome["status"] == "expired":
                    return self._row_result(r)
                line = render_outcome(outcome)
                rec = self._live.get(run_id)
                live = rec is not None and rec.driver is not None and not rec.driver.done()
                if r["taken_over"] or live or r["status"] not in ("waiting_human", "interrupted"):
                    self._queue_answer(r, outcome, line)
                    if not live:
                        return self._row_result(self.runs.row(run_id))
                    follow = True
                else:
                    rec = self._live_for(r, lease)
                    task = await self._grant_and_prepare(rec, outcome, line)
                    driver = self._launch(rec, task, lease)
                    launched = True
        finally:
            if not launched and lease is not None:  # unused: the caller's lease goes back
                try:
                    self._ledger.release(lease)
                except Exception:
                    log.warning("lease release failed for %s", run_id, exc_info=True)
        if follow:
            return await asyncio.shield(self.wait(run_id))
        return await self._drive_then_release(rec, driver)

    def _queue_answer(self, r, outcome: AskOutcome, line: str) -> None:
        """The run cannot take the answer now: keep it pending for the next delivery."""
        self.runs.set_pending(r["id"], [*self.runs.pending(r["id"]), line])
        self._emit(
            "run.answer_queued",
            {"ask_id": outcome["ask_id"]},
            run_id=r["id"],
            project_id=r["project_id"],
        )

    async def _grant_and_prepare(self, rec: LiveRun, outcome: AskOutcome, line: str) -> HarnessTask:
        """Grant an approving answer's token, then build the resume task; a failure after
        the grant takes the token back. Called with the run's lock held."""
        await self._drop_dead_session(rec)  # never grant on a dead guard
        token = await self._grant_approval(rec, outcome)
        try:
            task = await self._prepare_resume(rec, line)
            self._check_resumable(rec.run_id)  # nothing moved during the awaits
        except BaseException:
            if token is not None:
                self._drop_grant(rec, token)
            raise
        return task

    def _check_resumable(self, run_id: RunId) -> None:
        r = self.runs.row(run_id)
        rec = self._live.get(run_id)
        if (
            r is None
            or r["status"] not in ("waiting_human", "interrupted")
            or r["taken_over"]
            or (rec is not None and rec.driver is not None and not rec.driver.done())
            or (rec is not None and rec.cancel_reason is not None)
        ):
            raise IllegalTransition(f"run {run_id} changed while resuming")

    async def _stop_park_timer(self, rec: LiveRun) -> None:
        """Stop the 2 h close timer of a parked run, after a close already in flight."""
        if rec.tearing_down is not None:
            await asyncio.shield(rec.tearing_down)
        if rec.park_timer is not None and not rec.park_timer.done():
            rec.park_timer.cancel()
        rec.park_timer = None

    async def _prepare_resume(self, rec: LiveRun, line: str) -> HarnessTask:
        """Stop the 2 h close timer (respecting a close in flight) and build the task; a
        closed session is reopened by the driver and announced to the worker."""
        await self._stop_park_timer(rec)
        await self._drop_dead_session(rec)
        prefix = self._reopen_line(rec) if rec.session is None else None
        rec.asked = False
        # new human input = a new cycle: retry ladder and verify rejects start over (the
        # `attempts` counter keeps counting)
        self.runs.update(rec.run_id, retries=0, verify_rejects=0)
        return self._resume_task(rec, line, keep=True, prefix=prefix)

    def _resume_task(
        self, rec: LiveRun, line: str, *, fresh: bool = False, keep: bool = False, prefix=None
    ) -> HarnessTask:
        """HarnessTask continuing from the last checkpoint. `keep`: `line` comes from a
        human and stays pending until delivered; retry / verify lines are not kept."""
        cp = self.runs.checkpoint(rec.run_id) or {}
        pending = self.runs.pending(rec.run_id)
        parts = [*pending, line]
        if keep:
            self.runs.set_pending(rec.run_id, parts)
        rec.delivering = parts if keep else pending
        if prefix:
            parts = [prefix, *parts]
        task = self._build_task(rec.order)
        task["resume"] = HarnessResume(
            session_id=None if fresh else cp.get("session_id"),
            message="\n".join(parts),
            notes=list(cp.get("notes") or []),
            open_tabs=list(cp.get("open_tabs") or []),
        )
        return task

    def _live_for(self, r, lease: QuotaLease | None, *, require_lease: bool = True) -> LiveRun:
        """The in-memory record (rebuilt from the row after a restart), checked against the
        new lease."""
        rec = self._live.get(r["id"])
        order = rec.order if rec else order_from_json(r["order_json"])
        variant = rec.variant if rec else json.loads(r["variant_json"])
        adapter, engine, engine_id, browser = self._check(
            order, variant, lease, require_lease=require_lease
        )
        if rec is None:
            rec = LiveRun(
                run_id=r["id"],
                order=order,
                variant=variant,
                adapter=adapter,
                engine=engine,
                browser=browser,
                output_dir=self._output_root / r["id"],
                started=datetime.fromisoformat(r["started_at"]),
                lease=None,
                engine_id=engine_id,
            )
            self._live[r["id"]] = rec
        elif engine is not None:
            rec.engine, rec.engine_id = engine, engine_id
        return rec

    def _launch(self, rec: LiveRun, task: HarnessTask, lease: QuotaLease | None) -> asyncio.Task:
        rec.lease = lease
        rec.driver = self._spawn(rec, task)
        return rec.driver

    async def _drive_then_release(self, rec: LiveRun, driver: asyncio.Task) -> RunResult:
        try:
            return await driver
        finally:
            self._release_lease(rec)

    async def wait(self, run_id: RunId) -> RunResult:
        """Result of the run's current drive (e.g. the resume `release` started)."""
        rec = self._live.get(run_id)
        task = rec.driver if rec is not None else None
        if task is not None and not task.done():
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                me = asyncio.current_task()
                if me is not None and me.cancelling():
                    raise  # we are being cancelled ourselves
                # the drive itself was cancelled (e.g. its caller shut down): report the row
        r = self.runs.row(run_id)
        if r is None:
            raise KeyError(run_id)
        return self._row_result(r)

    # --- control -------------------------------------------------------------------

    def _lock(self, run_id: RunId) -> asyncio.Lock:
        return self._locks.setdefault(run_id, asyncio.Lock())

    @contextlib.asynccontextmanager
    async def _control(self, run_id: RunId):
        """Hold the run's lock; the entry is dropped once the run is finished and nobody
        holds or waits for it."""
        lock = self._lock(run_id)
        self._lock_users[run_id] = self._lock_users.get(run_id, 0) + 1
        try:
            async with lock:
                yield
        finally:
            left = self._lock_users[run_id] - 1
            if left:
                self._lock_users[run_id] = left
            else:
                del self._lock_users[run_id]
                if self._is_finished(run_id):
                    self._prune_lock(run_id)

    def _prune_lock(self, run_id: RunId) -> None:
        """Drop the run's lock entry when nobody holds or waits for it (a waiter must keep
        finding the same lock, or two holders could run side by side)."""
        lock = self._locks.get(run_id)
        if lock is not None and run_id not in self._lock_users and not lock.locked():
            del self._locks[run_id]

    @staticmethod
    async def _await_cleanup(task: asyncio.Task | None) -> None:
        """Wait for a cleanup task without cancelling it; re-raise only OUR cancellation."""
        if task is None:
            return
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            me = asyncio.current_task()
            if me is not None and me.cancelling():
                raise
        except Exception:
            log.warning("cancel cleanup failed", exc_info=True)

    async def steer(self, run_id: RunId, *, text: str) -> SteerMode:
        """A running harness with `steer` gets it at its next tool boundary; otherwise it is
        kept and delivered with the next resume."""
        r = self._unfinished(run_id)
        rec = self._live.get(run_id)
        delivered = False
        if (
            r["status"] == "running"
            and not r["taken_over"]
            and rec is not None
            and rec.driver is not None
            and not rec.driver.done()
            and "steer" in rec.adapter.capabilities
        ):
            try:
                await rec.adapter.steer(run_id, text)
                delivered = True
            except (UnknownHandle, NotSupported):
                log.info("run %s: harness not steerable right now; kept for resume", run_id)
        if not delivered:
            self.runs.set_pending(run_id, [*self.runs.pending(run_id), steer_line(text)])
        self.runs.update(run_id, human_intervened=1)
        mode: SteerMode = "at_tool_boundary"
        self._emit(
            "human.steer",
            {"text": text, "mode": mode, "delivered": "harness" if delivered else "on_resume"},
            run_id=run_id,
            project_id=r["project_id"],
        )
        return mode

    async def takeover(self, run_id: RunId) -> TakeoverInfo:
        """Pause the worker and hand its browser tab to a human: guard "off", no 2 h close."""
        async with self._control(run_id):
            return await self._takeover_locked(run_id)

    async def _takeover_locked(self, run_id: RunId) -> TakeoverInfo:
        r = self._unfinished(run_id)
        rec = self._live.get(run_id)
        driving = rec is not None and rec.driver is not None and not rec.driver.done()
        if driving or r["status"] == "running":
            # a live drive (also a resume still opening the browser while the row reads
            # waiting_human) is paused first: never a second session next to it
            if not driving:
                raise IllegalTransition(f"run {run_id} has no live worker to pause")
            await self._pause_driver(rec)
        elif r["status"] == "waiting_human":
            rec = self._live_for(r, None, require_lease=False)
            await self._stop_park_timer(rec)
        else:
            raise IllegalTransition(f"run {run_id} is {r['status']}: cannot take over")
        await self._drop_dead_session(rec)
        if rec.session is None or not rec.armed:  # closed after 2 h / restart / cut short
            await self._open_session(rec)  # reopen (or finish arming) the profile
        assert rec.guard is not None and rec.session is not None
        await rec.guard.set_mode(rec.session["id"], "off")
        self.runs.update(run_id, taken_over=1, human_intervened=1)
        info = self._takeover_info(rec)
        self._emit(
            "human.takeover",
            {"session": rec.session["id"], "url": info["url"], "profile": rec.order["profile"]},
            run_id=run_id,
            project_id=r["project_id"],
        )
        return info

    async def _pause_driver(self, rec: LiveRun) -> None:
        """Stop the live drive for a takeover: the driver parks the run instead of
        interrupting it. Raises unless the run ends up waiting_human."""
        assert rec.driver is not None
        rec.taking_over = True
        rec.driver.cancel()
        try:
            await self._await_cleanup(rec.driver)
        finally:
            rec.taking_over = False
        r = self.runs.row(rec.run_id)
        if r["status"] != "waiting_human":
            raise IllegalTransition(f"run {rec.run_id} ended {r['status']} before takeover")

    def _takeover_info(self, rec: LiveRun) -> TakeoverInfo:
        assert rec.session is not None
        tabs = (self.runs.checkpoint(rec.run_id) or {}).get("open_tabs") or []
        url = tabs[-1] if tabs else None
        return TakeoverInfo(
            run_id=rec.run_id,
            profile=rec.order["profile"],
            cdp_url=rec.session["cdp_url"],
            target_id=rec.session["target_id"],
            url=url,
            instructions=(
                f"{rec.variant['browser']} '{rec.order['profile']}' 프로필의 작업 탭"
                f" ({url or '현재 탭'})에서 직접 처리하고, 끝나면 '됐어'라고 알려줘."
                " 그동안 작업자는 멈추고 가드는 꺼져 있음."
            ),
        )

    async def _park_taken_over(self, rec: LiveRun) -> RunResult:
        """The driver was interrupted by takeover(): waiting_human, taken over, lease back."""
        r = self.runs.row(rec.run_id)
        if r["status"] not in ("running", "waiting_human"):
            return await self._finish(rec, "interrupted", f"takeover during {r['status']}")
        self._settle_pending(rec, delivered=False)
        summary = "사람이 넘겨받음"
        result = self._result(rec, "waiting_human", summary)
        self.runs.update(
            rec.run_id,
            taken_over=1,
            parked_at=self._clock.now().isoformat(),
            summary=summary,
            result_json=result_to_json(result),
        )
        if r["status"] == "running":  # (a resume caught while starting is still parked)
            self.runs.transition(rec.run_id, "waiting_human")
        self._release_lease(rec)
        self._emit(
            "run.parked",
            {"session": rec.session["id"] if rec.session else None, "taken_over": True},
            run_id=rec.run_id,
            project_id=rec.order["project_id"],
        )
        return result

    async def release(self, run_id: RunId, *, note: str) -> None:
        """The human is done: guard back to the order's mode, then resume in the background
        with "사람이 이렇게 했음: <note>" (`wait(run_id)` gives the result). Needs a free
        engine lease; without one nothing changes and LeaseExpired is raised."""
        async with self._control(run_id):
            return await self._release_locked(run_id, note=note)

    async def _release_locked(self, run_id: RunId, *, note: str) -> None:
        r = self._unfinished(run_id)
        if r["status"] != "waiting_human" or not r["taken_over"]:
            raise IllegalTransition(f"run {run_id} is not taken over")
        rec = self._live_for(r, None, require_lease=False)
        lease: QuotaLease | None = None
        if rec.adapter.needs_engine:
            lease = self._ledger.lease(
                engine_pref=[rec.engine_id or rec.variant["engine"]],
                purpose=rec.order["kind"],
                max_minutes=rec.order["budget"]["max_minutes"],
            )
            if lease is None:
                raise LeaseExpired("no engine lease free for release: try again later")
        try:
            rec = self._live_for(r, lease)
            if rec.session is not None and rec.guard is not None:
                await rec.guard.set_mode(rec.session["id"], rec.order["action_mode"])
        except BaseException:
            if lease is not None:
                self._ledger.release(lease)
            raise
        self.runs.update(run_id, taken_over=0, human_intervened=1)
        self._emit("human.release", {"note": note}, run_id=run_id, project_id=r["project_id"])
        task = await self._prepare_resume(rec, f"사람이 이렇게 했음: {note}")
        driver = self._launch(rec, task, lease)
        rec.releaser = asyncio.ensure_future(self._drive_then_release(rec, driver))

    async def cancel(self, run_id: RunId, *, reason: str) -> None:
        """Cancel from any unfinished state: stop the harness, expire Asks, disarm + close the
        session, status cancelled. A finished run is left as it is. The reason is set before
        waiting for the run's lock, so a takeover/resume holding it sees the cancel."""
        rec = self._live.get(run_id)
        mine = rec is not None and rec.cancel_reason is None and not self._is_finished(run_id)
        if mine:
            rec.cancel_reason = reason
        try:
            async with self._control(run_id):
                return await self._cancel_locked(run_id, reason=reason)
        except asyncio.CancelledError:
            # cancelled before the cleanup started (while queued for the lock): take the
            # reason back, or every later resume would be refused for a cancel never done
            if mine and not rec.cancel_started and rec.cancel_reason == reason:
                rec.cancel_reason = None
            raise

    async def _cancel_locked(self, run_id: RunId, *, reason: str) -> None:
        r = self.runs.row(run_id)
        if r is None:
            raise KeyError(run_id)
        if r["status"] in FINISHED:
            return
        rec = self._live.get(run_id)
        if rec is None:  # e.g. interrupted before a restart: nothing live to stop
            self._inbox.expire_for_run(run_id)
            self.runs.update(run_id, summary=clip(cancelled_summary(reason), SUMMARY_MAX))
            self.runs.transition(run_id, "cancelled")
            self._emit(
                "run.finished", {"status": "cancelled"}, run_id=run_id, project_id=r["project_id"]
            )
            return
        driving = rec.driver is not None and not rec.driver.done()
        if not rec.cancel_started:  # first cancel: start the cleanup exactly once
            rec.cancel_started = True
            rec.cancel_reason = rec.cancel_reason or reason
            if driving:
                rec.driver.cancel()
            else:
                rec.canceller = asyncio.ensure_future(self._finish_cancelled(rec))
        target = rec.driver if driving else rec.canceller
        await self._await_cleanup(target)

    # --- shutdown ------------------------------------------------------------------

    def background_tasks(self) -> list[asyncio.Task]:
        """Pool-owned tasks still pending: drivers, releasers, cancel cleanups, shielded
        teardowns and park timers. Nobody awaits most of them, so the daemon gathers them."""
        out: list[asyncio.Task] = []
        for rec in list(self._live.values()):
            for t in (rec.driver, rec.releaser, rec.canceller, rec.tearing_down, rec.park_timer):
                if t is not None and not t.done() and t not in out:
                    out.append(t)
        return out

    async def aclose(self) -> None:
        """Shutdown: a live driver is cancelled (no cancel reason -> the run ends
        `interrupted`, resumable after the restart); park timers stop; parked sessions are
        disarmed and closed (waiting_human runs stay waiting: a resume opens a new session);
        then every pool-owned background task is gathered. The caller bounds the time."""
        for rec in list(self._live.values()):
            if rec.park_timer is not None and not rec.park_timer.done():
                rec.park_timer.cancel()
            if rec.driver is not None and not rec.driver.done() and rec.cancel_reason is None:
                rec.driver.cancel()
        while pending := self.background_tasks():
            await asyncio.gather(*pending, return_exceptions=True)
            await asyncio.sleep(0)  # let done callbacks run
        for rec in list(self._live.values()):
            if rec.session is not None:
                await self._teardown(rec)  # never raises
        while pending := self.background_tasks():
            await asyncio.gather(*pending, return_exceptions=True)
