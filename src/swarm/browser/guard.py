"""ActionGuard: Fetch-domain gate on a session's outgoing requests.

The guard owns its own CDP connection (independent of the harness's), attaches to the
session's target in flatten mode and enables `Fetch` at request stage. Rules (the pure request
policy is in `browser/requests.py`):

- GET / HEAD / OPTIONS always pass.
- `guarded`: other methods pass when the host is allowlisted; otherwise they need an
  `ApprovalToken` (one token = one request, matched on method + host + path pattern).
  Without a token the request is failed (`BlockedByClient`) and `guard.blocked` is recorded.
  A token-approved request records `guard.executed` (dedupe key, fingerprint); the same
  fingerprint under the same key is never sent again — it is answered with a synthetic
  409 "already executed" (`guard.already_executed`). The key is the arm's `dedupe_key`
  (the pool passes `item:<backlog item>`, so a re-dispatched item under a new order id is
  covered) or else its order id. Executed fingerprints live in the EventLog, so this
  survives restarts.
- `dry_run`: every non-GET request is blocked; tokens are never used.
- `off`: everything passes (human takeover).

Coverage of new targets: the guard sets browser-level `Target.setAutoAttach`
(`waitForDebuggerOnStart`, flatten; pages, service workers, shared workers) on its connection,
so every such new target starts paused. Pages whose `openerId` is a guarded target (popups,
`target=_blank` forms) get Fetch enabled before they are resumed; other pages are resumed and
detached at once. Service/shared workers (any web scheme: http(s), blob, data, filesystem;
only browser-internal schemes are released) are registered while paused and their requests
are decided on a browser-level `Fetch.enable`, matched by `frameId` == worker target id. The
owner is chosen per request: no arm → pass; one arm → that arm's order; several → every
non-GET is blocked and no token is spent. Workers already running when first seen are closed
(Chrome does not intercept a running worker reliably). Page-level `Target.setAutoAttach`
on each guarded session covers OOPIFs and dedicated workers; guarded pages also get
`Network.setBypassServiceWorker(true)`. A child whose `Fetch.enable` fails is closed and
never resumed — except dedicated workers, which have no Fetch domain and whose requests the
owning page's Fetch session already pauses. Tabs a harness creates itself with
`Target.createTarget` have no opener — cover them with `arm_target`.

Browser-level Fetch is also the net under page-level Fetch: a guarded target whose page-level
`Fetch.enable` Chrome has not acknowledged yet (arming, or re-arming after a takeover) has its
browser-level pauses decided with the arm's rules instead of continued. `set_mode` returns only
once that acknowledgement arrived, and `session["guard"]` leaves "off" only then.

Fail-closed: if the guard's connection drops while arms are active, Chrome silently stops
intercepting for it, so the guard records `guard.disconnected` and calls `on_lost(targets)`;
`ChromeRuntime.open_guard` wires that to closing those targets (or killing the browser). The
stall watchdog runs that same closing step first and drops its connection afterwards, because
Chrome releases a paused request the moment its interceptor detaches. While that teardown
runs the socket still intercepts, so every non-safe request is failed and every new target is
closed; if the runtime could not close the targets (unknown profile, no callback), the guard
closes them over its own connection and records `guard.fail_closed_failed` for what remains.

Known gap: WebSocket / WebRTC sends are invisible to the Fetch domain.
"""

import asyncio
import base64
import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from swarm.browser.cdp import CdpClient, CdpClosed, CdpError
from swarm.browser.requests import (
    SAFE_METHODS,
    classify_request,
    describe_request,
    guardable_worker,
    token_matches,
)
from swarm.clock import Clock, SystemClock
from swarm.ports import ApprovalToken, RequestDescriptor
from swarm.store.events import Emitter, EventLog
from swarm.types import AskId, CdpSession, GuardMode, RunId, WorkOrderId

log = logging.getLogger(__name__)

STALL_AFTER_S = 5.0  # a paused request unanswered this long trips the watchdog
WATCHDOG_INTERVAL_S = 1.0
FETCH_PATTERNS = [{"urlPattern": "*", "requestStage": "Request"}]
WORKER_TYPES = frozenset({"service_worker", "shared_worker"})
DEDICATED_WORKER = "worker"
BROWSER_AUTO_ATTACH_FILTER = [{"type": t} for t in ("page", "service_worker", "shared_worker")]
UNATTRIBUTED = "(unattributed)"  # order id for workers no single arm can own
ALREADY_EXECUTED_BODY = (
    "already executed (이미 실행됨): this request was already sent once for this work item"
)

# --- guard -----------------------------------------------------------------------------


@dataclass
class _Arm:
    session_id: str  # CdpSession id ("" for the worker pool)
    target_id: str
    order_id: WorkOrderId
    run_id: RunId | None
    mode: GuardMode
    session: CdpSession | None = None
    dedupe_key: str = ""  # "already executed" scope; "" = the order id
    flat_sessions: set[str] = field(default_factory=set)
    flat_targets: dict[str, str] = field(default_factory=dict)  # flat session -> target id
    no_fetch: set[str] = field(default_factory=set)  # dedicated-worker sessions (no Fetch)
    pages: set[str] = field(default_factory=set)  # page targets to close if the guard is lost
    # targets whose page-level Fetch.enable Chrome has not acknowledged (never on, disabled
    # for takeover, or re-enable in flight): their browser-level pauses are decided here
    unfetched: set[str] = field(default_factory=set)

    @property
    def fetch_on(self) -> bool:
        """Page-level Fetch is acknowledged on every one of this arm's sessions."""
        return not self.unfetched


# (pages, workers) -> True when the callee closed (or killed) them; anything else means the
# guard must close them itself where it still can
LostCallback = Callable[[list[str], list[str]], Awaitable[bool | None] | bool | None]


class ActionGuard:
    def __init__(
        self,
        client: CdpClient,
        log: EventLog,
        allowlist: list[str],
        *,
        clock: Clock | None = None,
        on_lost: LostCallback | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        stall_after_s: float = STALL_AFTER_S,
    ) -> None:
        self._client = client
        self._log = log
        self.allowlist = list(allowlist)
        self._clock = clock or SystemClock()
        self._emit = Emitter(log, self._clock, "guard")
        self._on_lost = on_lost
        self._monotonic = monotonic
        self._stall_after_s = stall_after_s
        self._arms: dict[str, _Arm] = {}  # CdpSession id -> arm
        self._by_target: dict[str, _Arm] = {}  # guarded target ids (root, popups, frames)
        self._by_session: dict[str, _Arm] = {}  # guard-side flatten session ids
        self._tokens: list[ApprovalToken] = []
        self._arming = 0
        # browser-level machinery (auto-attach + Fetch) is on only while >= 1 arm is active
        self._epoch_on = False
        self._epoch_lock = asyncio.Lock()
        self._mode_lock = asyncio.Lock()
        # service/shared workers are owned by no arm: registered while paused, their requests
        # are decided at browser level with the owner taken from the arms active right now
        self._pool = _Arm(
            session_id="", target_id="", order_id=UNATTRIBUTED, run_id=None, mode="dry_run"
        )
        self._worker_targets: set[str] = set()
        self._pending: dict[tuple[str | None, str], float] = {}  # paused, not yet answered
        self._watchdog: asyncio.Task | None = None
        # stall teardown in progress: the socket still intercepts, so every non-safe request
        # is failed and every new target is closed until the armed targets are gone
        self._failing_closed = False
        client.on("Fetch.requestPaused", self._on_paused)
        client.on("Target.attachedToTarget", self._on_attached)
        client.on("Target.detachedFromTarget", self._on_detached)
        client.on_close(self._on_connection_lost)

    # --- public API ----------------------------------------------------------------------

    @property
    def connected(self) -> bool:
        """False once the guard's own CDP connection is gone (Chrome died, socket dropped):
        such a guard can never arm again and must be replaced by its owner."""
        return self._client.connected

    async def arm(
        self,
        session: CdpSession,
        *,
        order_id: WorkOrderId,
        mode: GuardMode,
        run_id: RunId | None = None,
        dedupe_key: str | None = None,
    ) -> None:
        """Start guarding the session's target. On success `session["guard"]` is set to `mode`;
        on failure nothing stays registered and the exception propagates. `dedupe_key` scopes
        "already executed" (default: the order id); approval tokens stay bound to the order."""
        if self._failing_closed:  # stall teardown: this guard is going away, never arm on it
            raise CdpClosed("guard is failing closed (stall teardown)")
        existing = self._arms.get(session["id"])
        if existing is not None:  # re-arm for another order/mode on the same session
            existing.order_id, existing.run_id = order_id, run_id
            existing.dedupe_key = dedupe_key or ""
            existing.session = session
            await self.set_mode(session["id"], mode)
            return
        a = _Arm(
            session_id=session["id"],
            target_id=session["target_id"],
            order_id=order_id,
            run_id=run_id,
            mode=mode,
            session=session,
            dedupe_key=dedupe_key or "",
        )
        await self._set_up(a)
        session["guard"] = mode
        if mode == "off":
            await self._apply_fetch(a)
        await self._sweep_workers()

    async def _set_up(self, a: _Arm) -> None:
        """Start the browser-level epoch, cover the arm's root target and register the arm. On
        failure nothing stays registered, the epoch ends if no other arm needs it, and the
        error propagates."""
        self._arming += 1  # worker requests count as unattributed until this arm is in place
        try:
            await self._start_epoch()
            await self._cover(a, a.target_id)
            if not self._client.connected:  # lost mid-arm: Chrome no longer intercepts for us
                self._forget(a)
                raise CdpClosed("guard connection lost while arming")
            if self._failing_closed:  # a stall teardown began mid-arm: its target snapshot
                self._forget(a)  # missed this tab, so it must not be left looking guarded
                raise CdpClosed("guard began failing closed while arming")
        except BaseException:
            self._arming -= 1
            if not self._arms and not self._arming:
                await self._end_epoch()
            raise
        self._register(a)

    def _register(self, a: _Arm) -> None:
        """Turn the arming arm into an active one in a single synchronous step. Between the
        decrement and the registration a request would see no arm and nothing arming, so a
        worker or frameless non-GET request would pass unguarded: never await in here."""
        self._arming -= 1
        self._arms[a.session_id] = a

    async def arm_target(self, session_id: str, target_id: str) -> None:
        """Guard an extra tab the harness opened itself (no opener link to follow)."""
        await self._cover(self._arms[session_id], target_id)

    async def set_mode(self, session_id: str, mode: GuardMode) -> None:
        """Switch mode. Decisions use the new mode at once; `off` (takeover) also disables
        page-level Fetch so the tabs are not paused. Leaving `off` re-enables it and returns
        only once Chrome has acknowledged (or the target was closed because it could not be
        re-guarded); until then the tab's requests are decided at browser level and
        `session["guard"]` still reads "off"."""
        a = self._arms[session_id]
        a.mode = mode
        if mode == "off" and a.session is not None:
            a.session["guard"] = "off"
        self._record(a, "guard.mode", {"order_id": a.order_id, "mode": mode})
        await self._apply_fetch(a)
        if a.session is not None and self._arms.get(session_id) is a and a.mode == mode:
            a.session["guard"] = mode

    def grant(self, token: ApprovalToken) -> None:
        """Let exactly one matching request of `token.order_id` through (in guarded mode).

        One Ask = one token: refused if the Ask already has a pending token here or an
        executed request in the EventLog."""
        if token.used:
            raise ValueError(f"approval token for {token.ask_id} already used")
        if any(t.ask_id == token.ask_id for t in self._tokens):
            raise ValueError(f"ask {token.ask_id} already has a pending token")
        if any(
            e["payload"].get("ask_id") == token.ask_id
            for e in self._log.query(kinds=["guard.executed"])
        ):
            raise ValueError(f"ask {token.ask_id} was already used for an executed request")
        self._tokens.append(token)

    def revoke(self, ask_id: AskId) -> bool:
        """Withdraw the Ask's token if it is still unused (the run ended without sending the
        request). True if a token was removed; a used or unknown token is a no-op."""
        for t in self._tokens:
            if t.ask_id == ask_id and not t.used:
                self._tokens.remove(t)
                return True
        return False

    async def disarm(self, session_id: str) -> None:
        """Stop guarding the session's own targets. Workers stay in the pool while other arms
        exist; disarming the last arm ends the browser-level epoch."""
        a = self._arms.pop(session_id, None)
        if a is None:
            return
        flats = list(a.flat_sessions)
        no_fetch = set(a.no_fetch)
        self._forget(a)
        for flat in flats:
            if flat not in no_fetch:
                await self._command("Fetch.disable", {}, flat)
            await self._command("Target.detachFromTarget", {"sessionId": flat}, None)
        if a.session is not None:
            a.session["guard"] = "off"
        if not self._arms and not self._arming:
            await self._end_epoch()

    async def close(self) -> None:
        """Disarm everything, then close the connection (no fail-closed action)."""
        for sid in list(self._arms):
            await self.disarm(sid)
        await self._client.close()

    async def check_stalled(self) -> bool:
        """Watchdog step: a paused request unanswered for > stall_after_s means the guard is
        not keeping up. Record `guard.stalled`, fail closed FIRST (armed pages and pool
        workers closed through `on_lost`, over the runtime's own connection), and only then
        drop the guard connection — Chrome releases paused requests on detach, so the order
        matters for the stalled request itself. The drop is a transport abort: no handshake
        to wait on."""
        if not self._pending or not self._client.connected:
            return False
        now = self._monotonic()
        ages = [now - t for t in self._pending.values()]
        stale = [x for x in ages if x > self._stall_after_s]
        if not stale:
            return False
        arms = list(self._arms.values())
        targets = sorted({t for a in arms for t in a.pages})
        workers = sorted(self._worker_targets)
        self._failing_closed = True  # from here on: fail everything, resume nothing
        self._report_stall(arms, targets, workers, ages, len(stale))
        try:
            if not await self._closed_by_runtime(targets, workers):
                await self._close_targets_ourselves(targets, workers)
        finally:
            self._take_down()  # state goes only now: until here the rules still applied
            self._pending.clear()
            self._client.abort()
        return True

    def _report_stall(
        self,
        arms: list[_Arm],
        targets: list[str],
        workers: list[str],
        ages: list[float],
        stale: int,
    ) -> None:
        self._emit(
            "guard.stalled",
            {
                "pending": len(self._pending),
                "stale": stale,
                "oldest_s": round(max(ages), 3),
                "targets": targets,
                "workers": workers,
                "orders": sorted({a.order_id for a in arms}),
            },
            run_id=None,
        )
        log.error(
            "guard stalled (%d requests > %ss); failing closed, then dropping connection",
            stale,
            self._stall_after_s,
        )

    async def _closed_by_runtime(self, targets: list[str], workers: list[str]) -> bool:
        """Stall teardown through `on_lost` (the runtime's own connection). True only when the
        callee reports it closed everything; any other outcome leaves it to the guard."""
        if not (targets or workers) or self._on_lost is None:
            return False
        try:
            res = self._on_lost(targets, workers)
            if inspect.isawaitable(res):
                res = await res
            return res is True
        except Exception:
            log.exception("guard: on_lost failed during stall teardown")
            return False

    async def _close_targets_ourselves(self, targets: list[str], workers: list[str]) -> None:
        """Stall fallback: the runtime closed nothing (unknown profile, no callback, error), so
        close the armed pages and pool workers over the guard's own, still-open connection."""
        unclosed: dict[str, list[str]] = {"targets": [], "workers": []}
        for kind, ids in (("targets", targets), ("workers", workers)):
            for tid in ids:
                try:
                    await self._client.send("Target.closeTarget", {"targetId": tid}, timeout_s=5)
                except (CdpError, ConnectionError, TimeoutError) as e:
                    log.error("guard: cannot close %s during stall teardown: %s", tid, e)
                    unclosed[kind].append(tid)
        if unclosed["targets"] or unclosed["workers"]:
            self._emit("guard.fail_closed_failed", unclosed, run_id=None)

    # --- browser-level epoch -----------------------------------------------------------------

    async def _start_epoch(self) -> None:
        async with self._epoch_lock:
            if self._epoch_on:
                return
            # Fetch first: nothing the auto-attach releases can slip out un-intercepted
            await self._client.send("Fetch.enable", {"patterns": FETCH_PATTERNS})
            self._epoch_on = True
            await self._client.send(
                "Target.setAutoAttach",
                {
                    "autoAttach": True,
                    "waitForDebuggerOnStart": True,
                    "flatten": True,
                    "filter": BROWSER_AUTO_ATTACH_FILTER,
                },
            )
            if self._watchdog is None or self._watchdog.done():
                self._watchdog = asyncio.ensure_future(self._watchdog_loop())

    async def _end_epoch(self) -> None:
        async with self._epoch_lock:
            if not self._epoch_on:
                return
            self._epoch_on = False
            await self._command(
                "Target.setAutoAttach",
                {"autoAttach": False, "waitForDebuggerOnStart": False, "flatten": True},
                None,
            )
            await self._command("Fetch.disable", {}, None)
            # workers lived through a period without browser-level Fetch: forget them; the
            # next epoch closes any that are still running (they are "pre-existing" then)
            for flat in [f for f, x in self._by_session.items() if x is self._pool]:
                del self._by_session[flat]
                await self._command("Target.detachFromTarget", {"sessionId": flat}, None)
            self._worker_targets.clear()
            if self._watchdog is not None:
                self._watchdog.cancel()
                self._watchdog = None

    async def _watchdog_loop(self) -> None:
        while self._client.connected:
            await asyncio.sleep(WATCHDOG_INTERVAL_S)
            try:
                if await self.check_stalled():
                    return
            except Exception:
                log.exception("guard watchdog failed")

    # --- coverage ------------------------------------------------------------------------

    def _forget(self, a: _Arm) -> None:
        for tid in [t for t, x in self._by_target.items() if x is a]:
            del self._by_target[tid]
        for flat in list(a.flat_sessions):
            self._by_session.pop(flat, None)
        a.flat_sessions.clear()
        a.flat_targets.clear()
        a.no_fetch.clear()
        a.pages.clear()
        a.unfetched.clear()

    def _track(self, a: _Arm, flat: str, tid: str) -> None:
        a.flat_sessions.add(flat)
        a.flat_targets[flat] = tid
        self._by_session[flat] = a

    def _untrack(self, a: _Arm, flat: str) -> None:
        a.flat_sessions.discard(flat)
        tid = a.flat_targets.pop(flat, None)
        if tid is not None and tid not in a.flat_targets.values():
            a.unfetched.discard(tid)
        a.no_fetch.discard(flat)
        self._by_session.pop(flat, None)

    async def _cover(self, a: _Arm, target_id: str) -> None:
        self._by_target[target_id] = a  # before attaching: our own attach event is ignored
        a.unfetched.add(target_id)  # browser level decides for it until Fetch is acked
        flat: str | None = None
        try:
            flat = await self._client.attach(target_id)
            self._track(a, flat, target_id)
            await self._client.send("Fetch.enable", {"patterns": FETCH_PATTERNS}, flat)
            a.unfetched.discard(target_id)
            await self._client.send("Network.setBypassServiceWorker", {"bypass": True}, flat)
            await self._client.send(
                "Target.setAutoAttach",
                {"autoAttach": True, "waitForDebuggerOnStart": True, "flatten": True},
                flat,
            )
        except BaseException:
            if self._by_target.get(target_id) is a:
                del self._by_target[target_id]
            a.unfetched.discard(target_id)
            if flat is not None:
                self._untrack(a, flat)
                await self._command("Target.detachFromTarget", {"sessionId": flat}, None)
            raise
        a.pages.add(target_id)

    async def _apply_fetch(self, a: _Arm) -> None:
        """Make page-level Fetch match the arm's mode (off during takeover). A target counts
        as covered by page-level Fetch only once Chrome acknowledged its `Fetch.enable`."""
        async with self._mode_lock:
            want = a.mode != "off"
            if a.session_id not in self._arms:
                return
            todo = [
                (flat, tid)
                for flat, tid in a.flat_targets.items()
                if flat not in a.no_fetch and (tid in a.unfetched) == want
            ]
            if not todo:
                return
            if not want:  # from here on nothing is assumed to be paused on these sessions
                a.unfetched.update(tid for _flat, tid in todo)
            for flat, tid in todo:
                if self._arms.get(a.session_id) is not a:
                    return  # disarmed meanwhile: disarm did its own cleanup, close nothing
                if not want:
                    await self._command("Fetch.disable", {}, flat)
                    continue
                try:
                    await self._client.send("Fetch.enable", {"patterns": FETCH_PATTERNS}, flat)
                except (CdpError, ConnectionError, TimeoutError) as e:  # cannot re-guard
                    if self._arms.get(a.session_id) is not a or flat not in a.flat_sessions:
                        return  # disarmed (or that target left) meanwhile: nothing to close
                    self._untrack(a, flat)
                    self._record(
                        a,
                        "guard.child_unguarded_closed",
                        {
                            "order_id": a.order_id,
                            "target_id": tid,
                            "type": "rearm",
                            "error": str(e),
                        },
                    )
                    await self._command("Target.closeTarget", {"targetId": tid}, None)
                    continue
                if self._arms.get(a.session_id) is a and flat in a.flat_sessions:
                    a.unfetched.discard(tid)

    async def _adopt(self, a: _Arm, flat: str, info: dict) -> None:
        """Guard a paused child target, then resume it. If it cannot be guarded, close it."""
        tid, ttype = info.get("targetId", ""), info.get("type", "")
        self._track(a, flat, tid)
        if ttype == DEDICATED_WORKER:
            # Chrome has no Fetch domain on dedicated workers (-32601); their requests are
            # paused on the owning page's Fetch session instead (integration-tested).
            a.no_fetch.add(flat)
            await self._command("Runtime.runIfWaitingForDebugger", {}, flat)
            return
        a.unfetched.add(tid)
        try:
            await self._client.send("Fetch.enable", {"patterns": FETCH_PATTERNS}, flat)
        except (CdpError, ConnectionError, TimeoutError) as e:
            self._untrack(a, flat)
            self._record(
                a,
                "guard.child_unguarded_closed",
                {"order_id": a.order_id, "target_id": tid, "type": ttype, "error": str(e)},
            )
            await self._command("Target.closeTarget", {"targetId": tid}, None)
            return  # never resumed
        if a.mode != "off":
            a.unfetched.discard(tid)
        await self._command(
            "Target.setAutoAttach",
            {"autoAttach": True, "waitForDebuggerOnStart": True, "flatten": True},
            flat,
        )
        await self._command("Runtime.runIfWaitingForDebugger", {}, flat)
        if ttype == "page":
            # after resuming: Network commands wait on the paused renderer
            await self._command("Network.setBypassServiceWorker", {"bypass": True}, flat)
        if a.mode == "off":  # adopted during takeover: not paused, like its parent
            a.unfetched.add(tid)
            await self._command("Fetch.disable", {}, flat)

    async def _sweep_workers(self) -> None:
        """Workers already running when first seen cannot be intercepted reliably: close them."""
        try:
            infos = (await self._client.send("Target.getTargets")).get("targetInfos", [])
        except (CdpError, ConnectionError) as e:
            log.warning("guard: worker sweep failed: %s", e)
            return
        for info in infos:
            tid = info.get("targetId", "")
            if (
                info.get("type") not in WORKER_TYPES
                or tid in self._worker_targets
                or not guardable_worker(info)
            ):
                continue
            self._worker_targets.add(tid)  # stays registered even if attaching fails
            try:
                flat = await self._client.attach(tid)
            except (CdpError, ConnectionError) as e:
                log.warning("guard: cannot attach existing worker %s: %s", tid, e)
                await self._close_preexisting(info)
                continue
            self._by_session[flat] = self._pool
            await self._adopt_worker(flat, info, waiting=False)

    async def _close_preexisting(self, info: dict) -> None:
        tid = info.get("targetId", "")
        closed = True
        try:
            await self._client.send("Target.closeTarget", {"targetId": tid})
        except (CdpError, ConnectionError) as e:
            closed = False
            log.warning("guard: cannot close pre-existing worker %s: %s", tid, e)
        self._emit(
            "guard.preexisting_worker_closed",
            {
                "target_id": tid,
                "type": info.get("type", ""),
                "url": info.get("url", ""),
                "closed": closed,
            },
            run_id=None,
        )

    async def _adopt_worker(self, flat: str, info: dict, *, waiting: bool) -> None:
        """Register a service/shared worker (or a worker nested in one); its requests are
        decided at browser level by `frameId`."""
        if not waiting:
            # Already running before we saw it: session-level Fetch on a running worker was
            # verified to miss its requests, and browser-level coverage of an already-running
            # worker is unverified. Close it; the next client starts a fresh instance, which is
            # auto-attached paused and guarded from its first request. It stays registered,
            # so if it cannot be closed its requests still go through the pool rule.
            await self._close_preexisting(info)
            return
        try:  # nested workers register through the same path
            await self._client.send(
                "Target.setAutoAttach",
                {"autoAttach": True, "waitForDebuggerOnStart": True, "flatten": True},
                flat,
            )
        except (CdpError, ConnectionError) as e:
            log.info("guard: worker %s refuses child auto-attach: %s", info.get("type"), e)
        await self._command("Runtime.runIfWaitingForDebugger", {}, flat)

    async def _release(self, flat: str, waiting: bool) -> None:
        if waiting:
            await self._command("Runtime.runIfWaitingForDebugger", {}, flat)
        await self._command("Target.detachFromTarget", {"sessionId": flat}, None)

    def _on_attached(self, params: dict, session_id: str | None) -> Awaitable[None] | None:
        info = params.get("targetInfo") or {}
        tid = info.get("targetId", "")
        child = params["sessionId"]
        waiting = bool(params.get("waitingForDebugger"))
        if self._failing_closed:  # teardown: nothing new may start running
            return self._command("Target.closeTarget", {"targetId": tid}, None)
        under_pool = session_id is not None and self._by_session.get(session_id) is self._pool
        if info.get("type") in WORKER_TYPES or under_pool:  # pool: service/shared (+ nested)
            if info.get("type") in WORKER_TYPES and session_id is not None and not under_pool:
                # a service worker is also reported under its page's session. Pool workers
                # register only from the browser-level attach (whose session outlives the
                # page); the page-level duplicate is just resumed — the worker starts only
                # once both waiting sessions resumed it, so this never runs it unregistered
                return self._release(child, waiting)
            if tid in self._worker_targets:
                # our own sweep attach (not waiting), or a second browser-level report
                return self._release(child, waiting) if waiting else None
            if not guardable_worker(info) and not under_pool:
                return self._release(child, waiting)
            self._worker_targets.add(tid)
            self._by_session[child] = self._pool
            return self._adopt_worker(child, info, waiting=waiting)
        if session_id is None:  # browser-level auto-attach: a page
            if tid in self._by_target:
                return None  # our own explicit attach to a guarded target
            parent = self._by_target.get(info.get("openerId") or "")
        else:  # page-level auto-attach under a guarded session (OOPIF, dedicated worker)
            parent = self._by_session.get(session_id)
        if parent is None:
            return self._release(child, waiting)
        self._by_target[tid] = parent  # register now: grandchildren may follow quickly
        if info.get("type") == "page":
            parent.pages.add(tid)  # fail-closed covers it even while setup is in flight
        self._track(parent, child, tid)
        return self._adopt(parent, child, info)

    def _on_detached(self, params: dict, session_id: str | None) -> None:
        flat = params.get("sessionId", "")
        a = self._by_session.get(flat)
        if a is None:
            return
        if a is self._pool:
            del self._by_session[flat]
            self._worker_targets.discard(params.get("targetId", ""))
            return
        self._untrack(a, flat)
        tid = params.get("targetId")
        if tid and self._by_target.get(tid) is a:
            del self._by_target[tid]
            a.pages.discard(tid)

    def _take_down(self) -> tuple[list[_Arm], list[str], list[str]]:
        """Forget every arm and pool worker (nothing intercepts for us any more) and return
        what has to be closed: the arms, their page targets and the pool workers."""
        arms = list(self._arms.values())
        targets = sorted({t for a in arms for t in a.pages})
        workers = sorted(self._worker_targets)
        self._worker_targets.clear()
        self._epoch_on = False
        if self._watchdog is not None and self._watchdog is not asyncio.current_task():
            self._watchdog.cancel()
        self._watchdog = None
        for a in arms:
            if a.session is not None:
                a.session["guard"] = "off"
        self._arms.clear()
        self._by_target.clear()
        self._by_session.clear()
        return arms, targets, workers

    def _on_connection_lost(self) -> Awaitable[None] | None:
        arms, targets, workers = self._take_down()
        self._pending.clear()
        if not targets:
            return None
        self._emit(
            "guard.disconnected",
            {"targets": targets, "workers": workers, "orders": sorted({a.order_id for a in arms})},
            run_id=None,
        )
        log.error("guard connection lost with %d guarded targets; failing closed", len(targets))
        return self._on_lost(targets, workers) if self._on_lost is not None else None

    # --- decisions -----------------------------------------------------------------------

    def _on_paused(self, params: dict, session_id: str | None) -> Awaitable[None]:
        key = (session_id, params["requestId"])
        self._pending[key] = self._monotonic()
        try:
            action = self._decide(params, session_id)
        except Exception:  # fail closed: a guard bug must not let a request through
            log.exception("guard: decision failed; blocking request")
            action = self._command(
                "Fetch.failRequest",
                {"requestId": params["requestId"], "errorReason": "BlockedByClient"},
                session_id,
            )
        return self._settle(key, action)

    async def _settle(self, key: tuple[str | None, str], action: Awaitable[None]) -> None:
        try:
            await action
        finally:
            self._pending.pop(key, None)

    def _active(self) -> bool:
        return bool(self._arms) or self._arming > 0

    def _rearming(self) -> bool:
        """Some guarded (non-off) target has no acknowledged page-level Fetch right now."""
        return any(a.mode != "off" and a.unfetched for a in self._by_target.values())

    def _decide(self, params: dict, session_id: str | None) -> Awaitable[None]:
        """Decide one paused request. Fully synchronous: everything that must not race (a
        token being spent, an execution being recorded) happens before the first await."""
        rid = params["requestId"]
        a = self._by_session.get(session_id or "")
        req = params["request"]
        unattributed = False
        if self._failing_closed and req["method"].upper() not in SAFE_METHODS:
            return self._block(self._pool, describe_request(req), rid, session_id, "failing_closed")
        if session_id is None:  # browser-level interception
            owner = self._owner_at_browser_level(params, req, rid)
            if not isinstance(owner, _Arm):
                return owner
            a = owner
        if a is self._pool:  # service/shared worker: owner from the arms active right now
            owned = self._owner_for_worker(rid, session_id)
            if not isinstance(owned, tuple):
                return owned
            a, unattributed = owned
        if a is None:  # a session we no longer track: only safe methods go through
            safe = req["method"].upper() in SAFE_METHODS
            if safe:
                return self._continue(rid, session_id)
            return self._command(
                "Fetch.failRequest",
                {"requestId": rid, "errorReason": "BlockedByClient"},
                session_id,
            )
        return self._decide_for_arm(a, req, rid, session_id, unattributed)

    def _owner_at_browser_level(self, params: dict, req: dict, rid: str) -> _Arm | Awaitable[None]:
        """The arm whose rules decide a browser-level pause (the pool for workers), or the
        response to return right away."""
        frame = params.get("frameId") or ""
        if not frame:
            if not self._active() or req["method"].upper() in SAFE_METHODS:
                return self._continue(rid, None)
            # no frame, no worker: nothing to attribute it to while something is guarded
            return self._block(self._pool, describe_request(req), rid, None, "no_frame")
        if frame in self._worker_targets:
            return self._pool
        owner = self._by_target.get(frame)
        if owner is None:
            if self._rearming() and req["method"].upper() not in SAFE_METHODS:
                # a frame that is no target (same-process subframe) while some guarded
                # target has no acked page-level Fetch: it may be that target's frame
                return self._block(self._pool, describe_request(req), rid, None, "unknown_frame")
            # frames of unarmed tabs are not guarded by design
            return self._continue(rid, None)
        if owner.mode == "off" or frame not in owner.unfetched:
            # guarded pages were already decided on their own session
            return self._continue(rid, None)
        return owner  # its page-level Fetch is not acked yet: decide here with its rules

    def _owner_for_worker(
        self, rid: str, session_id: str | None
    ) -> tuple[_Arm, bool] | Awaitable[None]:
        """(owner, unattributed) for a pool worker's request, or the response to return now.
        No arm: pass. One arm: that arm's order. Several, or one mid-setup: unattributed."""
        arms = list(self._arms.values())
        if self._arming:
            return self._pool, True  # an arm is mid-setup: owner unknown, no token use
        if not arms:
            return self._continue(rid, session_id)
        if len(arms) == 1:
            return arms[0], False
        return self._pool, True  # several orders could own it: no token may be spent

    def _decide_for_arm(
        self, a: _Arm, req: dict, rid: str, session_id: str | None, unattributed: bool
    ) -> Awaitable[None]:
        """Apply the arm's mode, allowlist, executed fingerprints and tokens to a request."""
        host = (urlsplit(req["url"]).hostname or "").lower()
        verdict = classify_request(a.mode, req["method"], host, self.allowlist)
        if verdict == "pass":
            return self._continue(rid, session_id)
        d = describe_request(req)
        if unattributed:
            return self._block(a, d, rid, session_id, "unattributed_worker")
        if verdict == "block":
            return self._block(a, d, rid, session_id, "dry_run")
        if self._already_executed(a.dedupe_key or a.order_id, d["fingerprint"]):
            return self._fulfill_already_executed(a, d, rid, session_id)
        token = next((t for t in self._tokens if token_matches(t, d, a.order_id)), None)
        if token is None:
            return self._block(a, d, rid, session_id, "needs_token")
        # decided synchronously (before any await): a token can never pass two requests
        token.used = True
        self._tokens.remove(token)
        self._record(
            a,
            "guard.executed",
            {
                "order_id": a.order_id,
                "dedupe_key": a.dedupe_key or a.order_id,
                "fingerprint": d["fingerprint"],
                "ask_id": token.ask_id,
                "method": d["method"],
                "host": d["host"],
                "path": d["path"],
            },
        )
        return self._continue(rid, session_id)

    def _fulfill_already_executed(
        self, a: _Arm, d: RequestDescriptor, rid: str, session_id: str | None
    ) -> Awaitable[None]:
        """Answer a request already sent once under this dedupe key with a synthetic 409."""
        self._record(
            a,
            "guard.already_executed",
            {
                "order_id": a.order_id,
                "dedupe_key": a.dedupe_key or a.order_id,
                "fingerprint": d["fingerprint"],
                "descriptor": d,
            },
        )
        return self._command(
            "Fetch.fulfillRequest",
            {
                "requestId": rid,
                "responseCode": 409,
                "responseHeaders": [
                    {"name": "Content-Type", "value": "text/plain; charset=utf-8"},
                    {"name": "Access-Control-Allow-Origin", "value": "*"},
                    {"name": "X-Swarm-Guard", "value": "already-executed"},
                ],
                "body": base64.b64encode(ALREADY_EXECUTED_BODY.encode()).decode(),
            },
            session_id,
        )

    def _continue(self, rid: str, session_id: str | None) -> Awaitable[None]:
        return self._command("Fetch.continueRequest", {"requestId": rid}, session_id)

    def _block(
        self, a: _Arm, d: RequestDescriptor, rid: str, session_id: str | None, reason: str
    ) -> Awaitable[None]:
        self._record(
            a,
            "guard.blocked",
            {"order_id": a.order_id, "mode": a.mode, "reason": reason, "descriptor": d},
        )
        return self._command(
            "Fetch.failRequest", {"requestId": rid, "errorReason": "BlockedByClient"}, session_id
        )

    async def _command(self, method: str, params: dict, session_id: str | None) -> None:
        try:
            await self._client.send(method, params, session_id)
        except (CdpError, ConnectionError, TimeoutError) as e:  # target closed mid-request
            log.info("guard: %s failed: %s", method, e)

    def _already_executed(self, key: str, fp: str) -> bool:
        """`key` = the arm's dedupe key; rows written before keys existed match on order id."""
        return any(
            e["payload"].get("dedupe_key", e["payload"].get("order_id")) == key
            and e["payload"].get("fingerprint") == fp
            for e in self._log.query(kinds=["guard.executed"])
        )

    def _record(self, a: _Arm, kind: str, payload: dict) -> None:
        self._emit(kind, payload, run_id=a.run_id)
