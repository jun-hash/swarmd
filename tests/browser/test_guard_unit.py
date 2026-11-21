"""ActionGuard state handling against a fake CDP client (no Chrome)."""

import asyncio
import inspect
from datetime import datetime

import pytest

from swarm.browser.cdp import CdpClosed, CdpError
from swarm.browser.guard import FETCH_PATTERNS, ActionGuard
from swarm.clock import SEOUL
from swarm.ports import ApprovalToken
from swarm.types import CdpSession, Event


class FakeCdp:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict, str | None]] = []
        self.fail: dict[str, int] = {}  # method -> remaining failures
        self.handlers: dict[str, list] = {}
        self.close_cbs: list = []
        self.connected = True
        self.results: dict[str, dict] = {}  # method -> canned result
        self.side_effects: dict[str, object] = {}  # method -> callable run on send
        self.gates: dict[tuple[str, str | None], asyncio.Event] = {}  # hold a reply until set
        self.attach_fail: set[str] = set()
        self.errors: dict[str, Exception] = {}  # method -> raised on every send
        self.aborted = False

    def on(self, event, cb):
        self.handlers.setdefault(event, []).append(cb)
        return lambda: None

    def on_close(self, cb):
        self.close_cbs.append(cb)

    async def send(self, method, params=None, session_id=None, *, timeout_s=None):
        self.sent.append((method, params or {}, session_id))
        gate = self.gates.get((method, session_id))
        if gate is not None:
            await gate.wait()
        if method in self.side_effects:
            self.side_effects[method]()
        if method in self.errors:
            raise self.errors[method]
        for key in ((method, session_id), method):  # (method, session) or any session
            if self.fail.get(key, 0) > 0:
                self.fail[key] -= 1
                raise CdpError(method, -32000, "injected failure")
        return self.results.get(method, {})

    async def attach(self, target_id):
        self.sent.append(("Target.attachToTarget", {"targetId": target_id}, None))
        if target_id in self.attach_fail:
            raise CdpError("Target.attachToTarget", -32000, "no such target")
        return f"FLAT-{target_id}"

    async def close(self):
        self.connected = False
        for cb in self.close_cbs:
            res = cb()
            if inspect.isawaitable(res):
                await res

    def abort(self):
        """Transport dropped without a close handshake; on_close fires like on a peer drop."""
        self.aborted = True
        self.connected = False
        for cb in self.close_cbs:
            res = cb()
            if inspect.isawaitable(res):
                asyncio.ensure_future(res)

    async def fire(self, event, params, session_id=None):
        for cb in self.handlers.get(event, []):
            res = cb(params, session_id)
            if inspect.isawaitable(res):
                await res

    def methods(self, session_id=None):
        return [m for m, _p, s in self.sent if session_id is None or s == session_id]


def session(sid="bs_1", target="T1") -> CdpSession:
    return CdpSession(
        id=sid,
        cdp_url="ws://fake",
        target_id=target,
        profile="test",
        capabilities=frozenset(),
        guard="off",
    )


async def test_failed_arm_leaves_no_state_and_retry_really_arms(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    s = session()
    cdp.fail[("Fetch.enable", "FLAT-T1")] = 1  # the page session's Fetch.enable
    with pytest.raises(CdpError):
        await g.arm(s, order_id="wo_1", mode="guarded")
    assert s["guard"] == "off"
    await g.arm(s, order_id="wo_1", mode="guarded")
    assert cdp.methods("FLAT-T1").count("Fetch.enable") == 2  # the retry enabled Fetch again
    assert s["guard"] == "guarded"
    await g.set_mode("bs_1", "off")
    assert s["guard"] == "off"


async def test_child_whose_fetch_enable_fails_is_closed_never_resumed(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    cdp.fail["Fetch.enable"] = 1
    await cdp.fire(
        "Target.attachedToTarget",
        {
            "sessionId": "CHILD",
            "targetInfo": {"targetId": "W1", "type": "iframe", "url": "http://x/frame"},
            "waitingForDebugger": True,
        },
        "FLAT-T1",
    )
    assert "Runtime.runIfWaitingForDebugger" not in cdp.methods("CHILD")
    assert ("Target.closeTarget", {"targetId": "W1"}, None) in cdp.sent
    ev = [e["payload"] for e in log.query(kinds=["guard.child_unguarded_closed"])]
    assert ev == [
        {"order_id": "wo_1", "target_id": "W1", "type": "iframe", "error": ev[0]["error"]}
    ]


async def test_dedicated_worker_is_resumed_without_own_fetch(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire(
        "Target.attachedToTarget",
        {
            "sessionId": "DW",
            "targetInfo": {"targetId": "W2", "type": "worker", "url": "http://x/w.js"},
            "waitingForDebugger": True,
        },
        "FLAT-T1",
    )
    assert cdp.methods("DW") == ["Runtime.runIfWaitingForDebugger"]


async def test_one_ask_is_one_token_even_across_guard_instances(log):
    log.append(
        Event(
            id="",
            ts=datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL),
            kind="guard.executed",
            actor="guard",
            project_id=None,
            run_id=None,
            payload={"order_id": "wo_1", "fingerprint": "f", "ask_id": "ask_used"},
            refs=[],
        )
    )
    fresh = ActionGuard(FakeCdp(), log, allowlist=[])
    with pytest.raises(ValueError):
        fresh.grant(ApprovalToken("ask_used", "POST", "h", "/p", order_id="wo_1"))
    fresh.grant(ApprovalToken("ask_new", "POST", "h", "/p", order_id="wo_1"))
    with pytest.raises(ValueError):  # a second token for the same Ask
        fresh.grant(ApprovalToken("ask_new", "POST", "h", "/q", order_id="wo_1"))


async def test_connection_loss_with_active_arms_reports_targets(log):
    cdp = FakeCdp()
    lost: list[list[str]] = []

    async def on_lost(targets, workers):
        lost.append(sorted(targets))

    g = ActionGuard(cdp, log, allowlist=[], on_lost=on_lost)
    await g.arm(session("bs_1", "T1"), order_id="wo_1", mode="guarded")
    await g.arm(session("bs_2", "T2"), order_id="wo_2", mode="guarded")
    for cb in cdp.close_cbs:
        res = cb()
        if inspect.isawaitable(res):
            await res
    assert lost == [["T1", "T2"]]
    ev = [e["payload"] for e in log.query(kinds=["guard.disconnected"])]
    assert ev and sorted(ev[0]["targets"]) == ["T1", "T2"]


async def test_close_after_disarm_does_not_fail_closed(log):
    cdp = FakeCdp()
    lost: list = []
    g = ActionGuard(cdp, log, allowlist=[], on_lost=lambda t, w: lost.append(t))
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await g.disarm("bs_1")
    for cb in cdp.close_cbs:
        cb()
    assert lost == [] and list(log.query(kinds=["guard.disconnected"])) == []


def worker_attached(sid="SW1", tid="W1", url="blob:http://127.0.0.1/x", ttype="shared_worker"):
    return {
        "sessionId": sid,
        "targetInfo": {"targetId": tid, "type": ttype, "url": url},
        "waitingForDebugger": True,
    }


def paused(rid="r1", method="POST", url="http://h.example/p", body="x", frame="W1"):
    return {
        "requestId": rid,
        "frameId": frame,
        "request": {"method": method, "url": url, "postData": body},
    }


def verdicts(cdp, sid=None):
    """Fetch decisions sent on `sid` (None = browser-level session)."""
    return [
        m
        for m, _p, s in cdp.sent
        if s == sid and m.startswith("Fetch.") and m not in ("Fetch.enable", "Fetch.disable")
    ]


@pytest.mark.parametrize(
    "url",
    [
        "blob:http://127.0.0.1:1/uuid",
        "data:text/javascript,1",
        "filesystem:http://h/x.js",
        "http://h/w.js",
        "https://h/w.js",
    ],
)
async def test_web_workers_of_any_scheme_are_guarded(log, url):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(url=url))
    # child auto-attach first (nested workers register the same way), then resume
    assert cdp.methods("SW1") == ["Target.setAutoAttach", "Runtime.runIfWaitingForDebugger"]
    await cdp.fire("Fetch.requestPaused", paused(frame="W1"))  # browser-level pause
    assert verdicts(cdp) == ["Fetch.failRequest"]


@pytest.mark.parametrize(
    "url", ["chrome-extension://abc/sw.js", "chrome://x/sw.js", "devtools://devtools/x.js"]
)
async def test_browser_internal_workers_are_released(log, url):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(url=url, ttype="service_worker"))
    assert ("Target.detachFromTarget", {"sessionId": "SW1"}, None) in cdp.sent
    await cdp.fire("Fetch.requestPaused", paused(frame="W1"))
    assert verdicts(cdp) == ["Fetch.continueRequest"]


async def test_browser_level_pauses_of_pages_pass_through(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    assert ("Fetch.enable", {"patterns": FETCH_PATTERNS}, None) in cdp.sent  # browser level
    # a page frame (guarded pages were decided on their own session; others are unguarded)
    await cdp.fire("Fetch.requestPaused", paused(frame="T1"))
    await cdp.fire("Fetch.requestPaused", paused(rid="r2", frame="SOME-OTHER-FRAME"))
    assert verdicts(cdp) == ["Fetch.continueRequest", "Fetch.continueRequest"]
    assert list(log.query(kinds=["guard.blocked"])) == []


async def test_worker_owner_is_decided_per_request_from_current_arms(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_a", "TA"), order_id="wo_a", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached())
    # one arm: the worker acts for that order
    await cdp.fire("Fetch.requestPaused", paused("r1"))
    assert verdicts(cdp) == ["Fetch.failRequest"]
    assert [e["payload"]["order_id"] for e in log.query(kinds=["guard.blocked"])] == ["wo_a"]
    # two arms: block every non-GET, never spend a token
    await g.arm(session("bs_b", "TB"), order_id="wo_b", mode="guarded")
    tok = ApprovalToken("ask_b", "POST", "h.example", "/p", order_id="wo_b")
    g.grant(tok)
    await cdp.fire("Fetch.requestPaused", paused("r2"))
    await cdp.fire("Fetch.requestPaused", paused("r3", method="GET"))
    assert verdicts(cdp)[1:] == ["Fetch.failRequest", "Fetch.continueRequest"]
    assert not tok.used
    assert list(log.query(kinds=["guard.blocked"]))[-1]["payload"]["reason"] == (
        "unattributed_worker"
    )
    # disarm A: the worker stays attached and now acts for B (token usable)
    await g.disarm("bs_a")
    assert ("Target.detachFromTarget", {"sessionId": "SW1"}, None) not in cdp.sent
    await cdp.fire("Fetch.requestPaused", paused("r4"))
    assert verdicts(cdp)[-1] == "Fetch.continueRequest" and tok.used
    # no arm at all: workers pass (nothing is being guarded)
    await g.disarm("bs_b")
    await cdp.fire("Fetch.requestPaused", paused("r5", body="other"))
    assert verdicts(cdp)[-1] == "Fetch.continueRequest"


async def test_arm_sweeps_existing_workers(log):
    cdp = FakeCdp()
    cdp.results["Target.getTargets"] = {
        "targetInfos": [
            {"targetId": "OLD", "type": "shared_worker", "url": "http://h/shared.js"},
            {"targetId": "EXT", "type": "service_worker", "url": "chrome-extension://e/sw.js"},
            {"targetId": "P", "type": "page", "url": "about:blank"},
        ]
    }
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    attached = [p["targetId"] for m, p, _s in cdp.sent if m == "Target.attachToTarget"]
    assert attached == ["T1", "OLD"]
    # a running worker cannot be intercepted: it is closed, never left running unguarded
    assert ("Target.closeTarget", {"targetId": "OLD"}, None) in cdp.sent
    assert "Runtime.runIfWaitingForDebugger" not in cdp.methods("FLAT-OLD")
    ev = [e["payload"]["target_id"] for e in log.query(kinds=["guard.preexisting_worker_closed"])]
    assert ev == ["OLD"]
    await g.arm(session("bs_2", "T2"), order_id="wo_2", mode="guarded")  # no double attach
    attached = [p["targetId"] for m, p, _s in cdp.sent if m == "Target.attachToTarget"]
    assert attached == ["T1", "OLD", "T2"]


async def test_popup_counts_for_fail_closed_while_still_being_set_up(log):
    cdp = FakeCdp()
    lost: list = []
    g = ActionGuard(cdp, log, allowlist=[], on_lost=lambda t, w: lost.append((t, w)))
    await g.arm(session(), order_id="wo_1", mode="guarded")
    params = {
        "sessionId": "POP",
        "targetInfo": {"targetId": "P2", "type": "page", "url": "", "openerId": "T1"},
        "waitingForDebugger": True,
    }
    setup = g._on_attached(params, None)  # adoption not yet run
    for cb in cdp.close_cbs:
        cb()
    setup.close()
    assert lost == [(["P2", "T1"], [])]


async def test_arm_raises_if_connection_dropped_during_cover(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])

    def drop():
        cdp.connected = False

    cdp.side_effects["Target.setAutoAttach"] = drop
    s = session()
    with pytest.raises(ConnectionError):
        await g.arm(s, order_id="wo_1", mode="guarded")
    assert s["guard"] == "off" and g._arms == {}


async def test_connection_loss_reports_guarded_workers(log):
    cdp = FakeCdp()
    lost: list = []
    g = ActionGuard(cdp, log, allowlist=[], on_lost=lambda t, w: lost.append((t, w)))
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(tid="W9"))
    for cb in cdp.close_cbs:
        cb()
    assert lost == [(["T1"], ["W9"])]
    ev = next(log.query(kinds=["guard.disconnected"]))["payload"]
    assert ev["workers"] == ["W9"]


# --- workers, browser-level Fetch and arm epochs ---------------------------------------


async def test_frameless_non_get_is_blocked_only_while_something_is_guarded(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Fetch.requestPaused", paused("r1", frame=""))
    await cdp.fire("Fetch.requestPaused", paused("r2", method="GET", frame=""))
    no_frame = paused("r3")
    del no_frame["frameId"]
    await cdp.fire("Fetch.requestPaused", no_frame)
    assert verdicts(cdp) == ["Fetch.failRequest", "Fetch.continueRequest", "Fetch.failRequest"]
    blocked = [e["payload"] for e in log.query(kinds=["guard.blocked"])]
    assert {(b["order_id"], b["reason"]) for b in blocked} == {("(unattributed)", "no_frame")}
    await g.disarm("bs_1")
    await cdp.fire("Fetch.requestPaused", paused("r4", frame=""))
    assert verdicts(cdp)[-1] == "Fetch.continueRequest"


async def test_sweep_keeps_worker_registered_and_closes_it_when_attach_fails(log):
    cdp = FakeCdp()
    cdp.results["Target.getTargets"] = {
        "targetInfos": [{"targetId": "OLD", "type": "service_worker", "url": "http://h/sw.js"}]
    }
    cdp.attach_fail.add("OLD")
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    assert ("Target.closeTarget", {"targetId": "OLD"}, None) in cdp.sent
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="OLD"))  # still pool-decided
    assert verdicts(cdp) == ["Fetch.failRequest"]


async def test_worker_nested_in_a_shared_worker_joins_the_pool(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached())
    nested = {
        "sessionId": "NEST",
        "targetInfo": {"targetId": "W-NEST", "type": "worker", "url": "blob:http://h/n"},
        "waitingForDebugger": True,
    }
    await cdp.fire("Target.attachedToTarget", nested, "SW1")
    assert "Runtime.runIfWaitingForDebugger" in cdp.methods("NEST")
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="W-NEST"))
    assert verdicts(cdp) == ["Fetch.failRequest"]


async def test_browser_level_fetch_only_while_an_arm_is_active(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_1", "T1"), order_id="wo_1", mode="guarded")
    root = [
        m
        for m, _p, sid in cdp.sent
        if sid is None and m in ("Fetch.enable", "Target.setAutoAttach")
    ]
    assert root == ["Fetch.enable", "Target.setAutoAttach"]  # Fetch before auto-attach
    await g.arm(session("bs_2", "T2"), order_id="wo_2", mode="guarded")
    await g.disarm("bs_1")
    assert ("Fetch.disable", {}, None) not in cdp.sent  # B still guarded
    await g.disarm("bs_2")
    assert ("Fetch.disable", {}, None) in cdp.sent
    off = [p for m, p, sid in cdp.sent if sid is None and m == "Target.setAutoAttach"][-1]
    assert off["autoAttach"] is False
    n = len(cdp.sent)
    await g.arm(session("bs_3", "T3"), order_id="wo_3", mode="guarded")  # next epoch
    assert ("Fetch.enable", {"patterns": FETCH_PATTERNS}, None) in cdp.sent[n:]


async def test_failed_first_arm_ends_the_epoch(log):
    cdp = FakeCdp()
    cdp.fail[("Fetch.enable", "FLAT-T1")] = 1
    g = ActionGuard(cdp, log, allowlist=[])
    with pytest.raises(CdpError):
        await g.arm(session(), order_id="wo_1", mode="guarded")
    assert ("Fetch.disable", {}, None) in cdp.sent


async def test_watchdog_drops_connection_when_a_decision_stalls(log):
    now = [100.0]
    cdp = FakeCdp()
    lost: list = []
    g = ActionGuard(
        cdp, log, allowlist=[], monotonic=lambda: now[0], on_lost=lambda t, w: lost.append(t)
    )
    await g.arm(session(), order_id="wo_1", mode="guarded")
    cdp.gates[("Fetch.continueRequest", "FLAT-T1")] = asyncio.Event()  # Chrome never answers
    stuck = asyncio.ensure_future(
        cdp.handlers["Fetch.requestPaused"][0](paused("r1", method="GET"), "FLAT-T1")
    )
    await asyncio.sleep(0)
    now[0] += 4.0
    assert await g.check_stalled() is False
    now[0] += 1.5
    assert await g.check_stalled() is True
    ev = [e["payload"] for e in log.query(kinds=["guard.stalled"])]
    assert ev and ev[0]["stale"] == 1 and ev[0]["oldest_s"] > 5
    assert cdp.connected is False and lost == [["T1"]]  # fail-closed path ran
    stuck.cancel()


async def test_takeover_disables_page_fetch_and_guarded_re_enables(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire(
        "Target.attachedToTarget",
        {
            "sessionId": "DW",
            "targetInfo": {"targetId": "W2", "type": "worker", "url": "http://x/w.js"},
            "waitingForDebugger": True,
        },
        "FLAT-T1",
    )
    await g.set_mode("bs_1", "off")
    assert ("Fetch.disable", {}, "FLAT-T1") in cdp.sent
    assert ("Fetch.disable", {}, "DW") not in cdp.sent  # dedicated workers have no Fetch
    n = len(cdp.sent)
    await g.set_mode("bs_1", "guarded")
    assert ("Fetch.enable", {"patterns": FETCH_PATTERNS}, "FLAT-T1") in cdp.sent[n:]
    await g.set_mode("bs_1", "dry_run")  # no Fetch churn between guarded modes
    assert [m for m, _p, sid in cdp.sent[n:] if sid == "FLAT-T1"] == ["Fetch.enable"]


async def test_takeover_re_enable_failure_closes_the_target(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await g.set_mode("bs_1", "off")
    cdp.fail[("Fetch.enable", "FLAT-T1")] = 1
    await g.set_mode("bs_1", "guarded")
    assert ("Target.closeTarget", {"targetId": "T1"}, None) in cdp.sent
    assert list(log.query(kinds=["guard.child_unguarded_closed"]))


async def test_worker_requests_while_an_arm_is_mid_setup_are_unattributed(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_1", "T1"), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached())
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_1"))
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T2")] = gate
    second = asyncio.ensure_future(g.arm(session("bs_2", "T2"), order_id="wo_2", mode="guarded"))
    await asyncio.sleep(0)
    await cdp.fire("Fetch.requestPaused", paused("r1"))
    assert verdicts(cdp) == ["Fetch.failRequest"]
    assert next(log.query(kinds=["guard.blocked"]))["payload"]["reason"] == "unattributed_worker"
    gate.set()
    await second


async def test_duplicate_waiting_attach_of_known_worker_is_resumed(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(ttype="service_worker"))
    dup = worker_attached(sid="DUP", ttype="service_worker")  # same worker, under the page
    await cdp.fire("Target.attachedToTarget", dup, "FLAT-T1")
    assert cdp.methods("DUP") == ["Runtime.runIfWaitingForDebugger"]
    assert ("Target.detachFromTarget", {"sessionId": "DUP"}, None) in cdp.sent


# --- stall fail-closed, re-arm and command timeouts ------------------------------------


async def test_set_mode_is_a_coroutine_and_guarded_waits_for_page_fetch(log):
    """off -> guarded: until Chrome acks the page-level Fetch.enable, the tab is covered by
    the browser-level decision with the arm's rules, and the session is not yet "guarded"."""
    assert asyncio.iscoroutinefunction(ActionGuard.set_mode)
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    s = session()
    await g.arm(s, order_id="wo_1", mode="guarded")
    await g.set_mode("bs_1", "off")
    assert s["guard"] == "off"
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T1")] = gate  # Chrome has not acked yet
    switch = asyncio.ensure_future(g.set_mode("bs_1", "guarded"))
    await asyncio.sleep(0)
    assert s["guard"] == "off" and not g._arms["bs_1"].fetch_on
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="T1"))  # browser-level pause
    await cdp.fire("Fetch.requestPaused", paused("r2", method="GET", frame="T1"))
    assert verdicts(cdp) == ["Fetch.failRequest", "Fetch.continueRequest"]
    blocked = [e["payload"] for e in log.query(kinds=["guard.blocked"])]
    assert [(b["order_id"], b["reason"]) for b in blocked] == [("wo_1", "needs_token")]
    gate.set()
    await switch
    assert s["guard"] == "guarded" and g._arms["bs_1"].fetch_on
    await cdp.fire("Fetch.requestPaused", paused("r3", frame="T1"))  # page level decides now
    assert verdicts(cdp)[-1] == "Fetch.continueRequest"


async def test_stall_fails_closed_before_dropping_the_guard_connection(log):
    now = [100.0]
    cdp = FakeCdp()
    seen: list = []

    async def on_lost(targets, workers):
        stalled = list(log.query(kinds=["guard.stalled"]))
        seen.append((sorted(targets), sorted(workers), cdp.connected, len(stalled)))

    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0], on_lost=on_lost)
    s = session()
    await g.arm(s, order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(tid="W1"))
    cdp.gates[("Fetch.continueRequest", "FLAT-T1")] = asyncio.Event()  # Chrome never answers
    stuck = asyncio.ensure_future(
        cdp.handlers["Fetch.requestPaused"][0](paused("r1", method="GET"), "FLAT-T1")
    )
    await asyncio.sleep(0)
    now[0] += 6.0
    assert await g.check_stalled() is True
    # targets were closed (on_lost ran) while the guard socket was still up, after the event
    assert seen == [(["T1"], ["W1"], True, 1)]
    assert cdp.aborted and cdp.connected is False  # dropped without a close handshake
    assert s["guard"] == "off" and g._arms == {}
    await asyncio.sleep(0)
    assert len(seen) == 1  # the connection-loss callback does not fail closed a second time
    stuck.cancel()


async def test_service_worker_reported_under_page_first_stays_in_pool_after_disarm(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_a", "TA"), order_id="wo_a", mode="guarded")
    await g.arm(session("bs_b", "TB"), order_id="wo_b", mode="guarded")
    # page-level attach (under A's session) arrives before the browser-level one
    dup = worker_attached(sid="DUP", tid="W1", url="http://h/sw.js", ttype="service_worker")
    await cdp.fire("Target.attachedToTarget", dup, "FLAT-TA")
    assert cdp.methods("DUP") == ["Runtime.runIfWaitingForDebugger"]
    assert ("Target.detachFromTarget", {"sessionId": "DUP"}, None) in cdp.sent
    top = worker_attached(sid="SW1", tid="W1", url="http://h/sw.js", ttype="service_worker")
    await cdp.fire("Target.attachedToTarget", top)
    assert "Runtime.runIfWaitingForDebugger" in cdp.methods("SW1")
    # A's page goes away (Chrome also reports the page-level session gone)
    await g.disarm("bs_a")
    await cdp.fire("Target.detachedFromTarget", {"sessionId": "DUP", "targetId": "W1"})
    await cdp.fire("Target.detachedFromTarget", {"sessionId": "FLAT-TA", "targetId": "TA"})
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="W1"))  # the worker still runs
    assert verdicts(cdp) == ["Fetch.failRequest"]
    assert [e["payload"]["order_id"] for e in log.query(kinds=["guard.blocked"])] == ["wo_b"]
    # the worker really ends: now it is forgotten
    await cdp.fire("Target.detachedFromTarget", {"sessionId": "SW1", "targetId": "W1"})
    assert "W1" not in g._worker_targets


async def test_rearm_failure_after_disarm_does_not_close_the_target(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await g.set_mode("bs_1", "off")
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T1")] = gate
    switch = asyncio.ensure_future(g.set_mode("bs_1", "guarded"))
    await asyncio.sleep(0)
    await g.disarm("bs_1")  # the harness gave the session up while the re-enable is in flight
    cdp.fail[("Fetch.enable", "FLAT-T1")] = 1
    gate.set()
    await switch
    assert ("Target.closeTarget", {"targetId": "T1"}, None) not in cdp.sent
    assert list(log.query(kinds=["guard.child_unguarded_closed"])) == []


async def test_command_timeout_is_logged_not_raised(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    cdp.errors["Fetch.continueRequest"] = TimeoutError("no reply")
    await cdp.fire("Fetch.requestPaused", paused("r1", method="GET"), "FLAT-T1")  # no raise
    assert verdicts(cdp, "FLAT-T1") == ["Fetch.continueRequest"]


# --- stall teardown and unknown-frame blocking -----------------------------------------


def popup_attached(sid="POP", tid="P2", opener="T1"):
    return {
        "sessionId": sid,
        "targetInfo": {"targetId": tid, "type": "page", "url": "", "openerId": opener},
        "waitingForDebugger": True,
    }


async def stall(g, cdp, now):
    """Park one GET decision on FLAT-T1 forever and trip the watchdog."""
    cdp.gates[("Fetch.continueRequest", "FLAT-T1")] = asyncio.Event()
    stuck = asyncio.ensure_future(
        cdp.handlers["Fetch.requestPaused"][0](paused("r-stuck", method="GET"), "FLAT-T1")
    )
    await asyncio.sleep(0)
    now[0] += 6.0
    assert await g.check_stalled() is True
    stuck.cancel()


async def test_stall_teardown_blocks_everything_until_targets_are_closed(log):
    """While on_lost closes targets the guard socket still intercepts: every non-GET must be
    failed and every new target closed, not resumed."""
    now = [100.0]
    cdp = FakeCdp()
    during: list = []

    async def on_lost(targets, workers):
        n = len(cdp.sent)
        await cdp.fire("Fetch.requestPaused", paused("rw", frame="W1"))  # SW POST
        await cdp.fire("Fetch.requestPaused", paused("rn", frame=""))  # frameless POST
        await cdp.fire("Fetch.requestPaused", paused("rp", frame="T1"), "FLAT-T1")  # page POST
        await cdp.fire("Fetch.requestPaused", paused("rg", method="GET", frame="W1"))
        await cdp.fire("Target.attachedToTarget", popup_attached())
        during.extend((m, s) for m, _p, s in cdp.sent[n:] if not m.startswith("Fetch.enable"))

    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0], on_lost=on_lost)
    s = session()
    await g.arm(s, order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(tid="W1"))
    await stall(g, cdp, now)
    assert during == [
        ("Fetch.failRequest", None),
        ("Fetch.failRequest", None),
        ("Fetch.failRequest", "FLAT-T1"),
        ("Fetch.continueRequest", None),
        ("Target.closeTarget", None),
    ]
    assert ("Target.closeTarget", {"targetId": "P2"}, None) in cdp.sent
    assert "Runtime.runIfWaitingForDebugger" not in cdp.methods("POP")
    assert cdp.aborted and g._arms == {} and s["guard"] == "off"


async def test_stall_closes_targets_over_the_guard_connection_when_runtime_cannot(log):
    now = [100.0]
    cdp = FakeCdp()

    async def on_lost(targets, workers):
        return False  # e.g. the runtime does not know the profile

    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0], on_lost=on_lost)
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached(tid="W1"))
    await stall(g, cdp, now)
    closed = [p["targetId"] for m, p, _s in cdp.sent if m == "Target.closeTarget"]
    assert sorted(closed) == ["T1", "W1"] and cdp.aborted
    assert list(log.query(kinds=["guard.fail_closed_failed"])) == []


async def test_stall_records_failure_when_nothing_can_close_the_targets(log):
    now = [100.0]
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0])  # no on_lost at all
    await g.arm(session(), order_id="wo_1", mode="guarded")
    cdp.errors["Target.closeTarget"] = CdpError("Target.closeTarget", -32000, "refused")
    await stall(g, cdp, now)
    assert ("Target.closeTarget", {"targetId": "T1"}, None) in cdp.sent
    ev = [e["payload"] for e in log.query(kinds=["guard.fail_closed_failed"])]
    assert ev and ev[0]["targets"] == ["T1"]
    assert cdp.aborted


async def test_child_fetch_enable_timeout_closes_the_child_and_the_next_one_too(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    cdp.errors["Fetch.enable"] = TimeoutError("no reply")
    for sid, tid in (("CA", "F1"), ("CB", "F2")):
        await cdp.fire(
            "Target.attachedToTarget",
            {
                "sessionId": sid,
                "targetInfo": {"targetId": tid, "type": "iframe", "url": "http://x/f"},
                "waitingForDebugger": True,
            },
            "FLAT-T1",
        )
        assert "Runtime.runIfWaitingForDebugger" not in cdp.methods(sid)
        assert ("Target.closeTarget", {"targetId": tid}, None) in cdp.sent
    kinds = [e["payload"]["target_id"] for e in log.query(kinds=["guard.child_unguarded_closed"])]
    assert kinds == ["F1", "F2"]
    # the same timeout on a takeover re-enable closes the target too
    del cdp.errors["Fetch.enable"]
    await g.set_mode("bs_1", "off")
    cdp.errors["Fetch.enable"] = TimeoutError("no reply")
    await g.set_mode("bs_1", "guarded")
    assert ("Target.closeTarget", {"targetId": "T1"}, None) in cdp.sent


async def test_unknown_frame_non_get_is_blocked_while_a_target_is_unacked(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Fetch.requestPaused", paused("r0", frame="SUBFRAME"))  # all acked: pass
    assert verdicts(cdp) == ["Fetch.continueRequest"]
    await g.set_mode("bs_1", "off")
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T1")] = gate
    switch = asyncio.ensure_future(g.set_mode("bs_1", "guarded"))
    await asyncio.sleep(0)
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="SUBFRAME"))  # same-process iframe
    await cdp.fire("Fetch.requestPaused", paused("r2", method="GET", frame="SUBFRAME"))
    assert verdicts(cdp)[1:] == ["Fetch.failRequest", "Fetch.continueRequest"]
    blocked = [e["payload"] for e in log.query(kinds=["guard.blocked"])]
    assert [(b["order_id"], b["reason"]) for b in blocked] == [("(unattributed)", "unknown_frame")]
    gate.set()
    await switch
    await cdp.fire("Fetch.requestPaused", paused("r3", frame="SUBFRAME"))
    assert verdicts(cdp)[-1] == "Fetch.continueRequest"


async def test_revoke_unused_token_blocks_the_request_again(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_1"))
    assert g.revoke("ask_1") is True
    await cdp.fire("Fetch.requestPaused", paused(frame="T1"), "FLAT-T1")
    assert verdicts(cdp, "FLAT-T1") == ["Fetch.failRequest"]
    assert g.revoke("ask_1") is False  # nothing left to revoke
    assert g.revoke("ask_nope") is False


async def test_revoke_after_use_is_false(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_1"))
    await cdp.fire("Fetch.requestPaused", paused(frame="T1"), "FLAT-T1")
    assert verdicts(cdp, "FLAT-T1") == ["Fetch.continueRequest"]
    assert g.revoke("ask_1") is False


async def test_connected_follows_the_guard_connection(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    assert g.connected is True
    cdp.connected = False  # Chrome died / the guard's socket dropped
    assert g.connected is False


async def test_already_executed_is_keyed_on_the_backlog_item_across_orders(log):
    """A re-dispatched item runs under a new order id; the request its first run sent
    must not go out again, and the new order's token is not spent on it."""
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_a", "TA"), order_id="wo_A", mode="guarded", dedupe_key="item:b1")
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_A"))
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="TA"), "FLAT-TA")
    assert verdicts(cdp, "FLAT-TA") == ["Fetch.continueRequest"]
    await g.disarm("bs_a")
    await g.arm(session("bs_b", "TB"), order_id="wo_B", mode="guarded", dedupe_key="item:b1")
    g.grant(ApprovalToken("ask_2", "POST", "h.example", "/p", order_id="wo_B"))
    await cdp.fire("Fetch.requestPaused", paused("r2", frame="TB"), "FLAT-TB")
    assert verdicts(cdp, "FLAT-TB") == ["Fetch.fulfillRequest"]  # synthetic 409
    [dup] = [e["payload"] for e in log.query(kinds=["guard.already_executed"])]
    assert dup["order_id"] == "wo_B" and dup["dedupe_key"] == "item:b1"
    assert g.revoke("ask_2") is True  # the new order's token was not spent


async def test_already_executed_without_a_key_stays_per_order(log):
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session("bs_a", "TA"), order_id="wo_A", mode="guarded")
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_A"))
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="TA"), "FLAT-TA")
    await g.disarm("bs_a")
    await g.arm(session("bs_b", "TB"), order_id="wo_B", mode="guarded")
    await cdp.fire("Fetch.requestPaused", paused("r2", frame="TB"), "FLAT-TB")
    assert verdicts(cdp, "FLAT-TB") == ["Fetch.failRequest"]  # needs its own approval


async def test_arm_during_a_stall_teardown_is_refused(log):
    """Ledger 78: a second run arming while the stall teardown runs would be registered after
    the target snapshot and cleared unclosed: arm must refuse instead."""
    now = [100.0]
    cdp = FakeCdp()
    outcome: list = []
    s2 = session("bs_2", "T2")

    async def on_lost(targets, workers):
        try:
            await g.arm(s2, order_id="wo_2", mode="guarded")
            outcome.append("armed")
        except CdpClosed as e:
            outcome.append(e)

    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0], on_lost=on_lost)
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await stall(g, cdp, now)
    assert len(outcome) == 1 and isinstance(outcome[0], CdpClosed)
    assert s2["guard"] == "off" and g._arms == {}


async def test_arm_that_straddles_the_start_of_a_stall_teardown_is_refused(log):
    now = [100.0]
    cdp = FakeCdp()
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T2")] = gate  # the 2nd arm is mid-cover
    s2 = session("bs_2", "T2")
    g = ActionGuard(cdp, log, allowlist=[], monotonic=lambda: now[0])
    arming: list[asyncio.Task] = []

    async def on_lost(targets, workers):
        gate.set()  # the arm finishes its cover while the teardown is running
        await asyncio.gather(arming[0], return_exceptions=True)
        return True

    g._on_lost = on_lost
    await g.arm(session(), order_id="wo_1", mode="guarded")
    arming.append(asyncio.ensure_future(g.arm(s2, order_id="wo_2", mode="guarded")))
    await asyncio.sleep(0)
    await stall(g, cdp, now)
    with pytest.raises(CdpClosed):
        await arming[0]
    assert s2["guard"] == "off"


async def test_a_token_passes_exactly_one_of_two_concurrent_requests(log):
    """The token is spent inside the synchronous decision: two matching requests paused back
    to back, with no yield between them, cannot both see it unused."""
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    g.grant(ApprovalToken("ask_1", "POST", "h.example", "/p", order_id="wo_1"))
    [on_paused] = cdp.handlers["Fetch.requestPaused"]
    first = on_paused(paused("r1", frame="T1"), "FLAT-T1")
    second = on_paused(paused("r2", body="y", frame="T1"), "FLAT-T1")  # not a duplicate
    await asyncio.gather(first, second)
    executed = [e["payload"] for e in log.query(kinds=["guard.executed"])]
    blocked = [e["payload"] for e in log.query(kinds=["guard.blocked"])]
    assert len(executed) == 1 and executed[0]["ask_id"] == "ask_1"
    assert [b["reason"] for b in blocked] == ["needs_token"]
    assert sorted(verdicts(cdp, "FLAT-T1")) == ["Fetch.continueRequest", "Fetch.failRequest"]


async def test_no_request_passes_while_an_arm_turns_active(log):
    """An arm leaves `_arming` and enters `_arms` in one synchronous step. A worker or
    frameless POST paused on every loop tick of the arm is blocked each time, never passed."""
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    gate = asyncio.Event()
    cdp.gates[("Fetch.enable", "FLAT-T1")] = gate  # hold the arm mid-cover
    arming = asyncio.ensure_future(g.arm(session(), order_id="wo_1", mode="guarded"))
    await asyncio.sleep(0)
    await cdp.fire("Target.attachedToTarget", worker_attached())  # a worker joins mid-arm
    done = asyncio.Event()

    async def keep_pausing():
        n = 0
        while not done.is_set():
            n += 1
            await cdp.fire("Fetch.requestPaused", paused(f"w{n}", frame="W1"))
            await cdp.fire("Fetch.requestPaused", paused(f"f{n}", frame=""))
            await asyncio.sleep(0)

    pausing = asyncio.ensure_future(keep_pausing())
    await asyncio.sleep(0)
    gate.set()
    await arming
    done.set()
    await pausing
    assert len(verdicts(cdp)) >= 4  # paused both while arming and once the arm was active
    assert set(verdicts(cdp)) == {"Fetch.failRequest"}


async def test_worker_request_passes_once_the_last_arm_is_gone(log):
    """Disarming the last arm leaves its workers registered until the epoch ends; a request
    of theirs in between has no order to act for and passes, like any unguarded traffic."""
    cdp = FakeCdp()
    g = ActionGuard(cdp, log, allowlist=[])
    await g.arm(session(), order_id="wo_1", mode="guarded")
    await cdp.fire("Target.attachedToTarget", worker_attached())
    gate = asyncio.Event()
    cdp.gates[("Fetch.disable", "FLAT-T1")] = gate  # hold the disarm before the epoch ends
    disarming = asyncio.ensure_future(g.disarm("bs_1"))
    await asyncio.sleep(0)
    await cdp.fire("Fetch.requestPaused", paused("r1", frame="W1"))
    assert verdicts(cdp) == ["Fetch.continueRequest"]
    assert list(log.query(kinds=["guard.blocked"])) == []
    gate.set()
    await disarming
