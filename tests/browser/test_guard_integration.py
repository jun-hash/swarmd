"""ActionGuard against a real Chrome.

Step 1 spike — two independent CDP clients on one page target:
    client A (harness role) drives the page over its own websocket; client B (guard role)
    opens a second websocket, attaches (flatten) to the same target and enables `Fetch`
    with `requestStage=Request`. B receives `Fetch.requestPaused` for A's form POST and
    `Fetch.failRequest` keeps it from reaching the local server.
    Result: PASS on Google Chrome 156.0.8078.80 (macOS, --headless=new), 2026-11-06.
"""

import asyncio
import contextlib
import hashlib
import itertools
import json
import os
import shutil
import signal
import subprocess
import tempfile
from pathlib import Path

import pytest
import websockets

from swarm.ports import ApprovalToken

from .guard_server import GuardServer
from .pages import Harness

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not CHROME.exists(), reason="Google Chrome not installed"),
]


class RawCdp:
    """Deliberately minimal CDP client for the spike (independent of swarm.browser.cdp)."""

    def __init__(self, ws) -> None:
        self.ws = ws
        self.ids = itertools.count(1)
        self.pending: dict[int, asyncio.Future] = {}
        self.handlers: list = []
        self.reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        async for raw in self.ws:
            msg = json.loads(raw)
            if "id" in msg:
                fut = self.pending.pop(msg["id"], None)
                if fut and not fut.done():
                    fut.set_result(msg)
            else:
                for h in self.handlers:
                    h(msg)

    async def send(self, method: str, params: dict | None = None, sid: str | None = None):
        mid = next(self.ids)
        fut = asyncio.get_running_loop().create_future()
        self.pending[mid] = fut
        msg = {"id": mid, "method": method, "params": params or {}}
        if sid:
            msg["sessionId"] = sid
        await self.ws.send(json.dumps(msg))
        resp = await asyncio.wait_for(fut, 10)
        assert "error" not in resp, resp
        return resp["result"]

    async def close(self) -> None:
        self.reader.cancel()
        await self.ws.close()


async def test_spike_second_client_fetch_blocks_form_post():
    server = GuardServer().start()
    profile = Path(tempfile.mkdtemp(prefix="swarm-spike-"))
    proc = subprocess.Popen(
        [
            str(CHROME),
            "--headless=new",
            "--remote-debugging-port=0",
            f"--user-data-dir={profile}",
            "--no-first-run",
            "--no-default-browser-check",
            "about:blank",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    a = b = None
    try:
        port_file = profile / "DevToolsActivePort"
        for _ in range(200):
            if port_file.exists() and len(port_file.read_text().splitlines()) >= 2:
                break
            await asyncio.sleep(0.05)
        port, path = port_file.read_text().splitlines()[:2]
        ws_url = f"ws://127.0.0.1:{port}{path}"

        a = RawCdp(await websockets.connect(ws_url, max_size=None))
        b = RawCdp(await websockets.connect(ws_url, max_size=None))
        version = (await a.send("Browser.getVersion"))["product"]
        target = (await a.send("Target.createTarget", {"url": "about:blank"}))["targetId"]
        sid_a = (await a.send("Target.attachToTarget", {"targetId": target, "flatten": True}))[
            "sessionId"
        ]
        sid_b = (await b.send("Target.attachToTarget", {"targetId": target, "flatten": True}))[
            "sessionId"
        ]

        paused: list[dict] = []

        def guard(msg: dict) -> None:
            if msg.get("method") != "Fetch.requestPaused":
                return
            p = msg["params"]
            paused.append(p["request"])
            if p["request"]["method"] == "GET":
                call = b.send("Fetch.continueRequest", {"requestId": p["requestId"]}, sid_b)
            else:
                call = b.send(
                    "Fetch.failRequest",
                    {"requestId": p["requestId"], "errorReason": "BlockedByClient"},
                    sid_b,
                )
            asyncio.ensure_future(call)

        b.handlers.append(guard)
        await b.send(
            "Fetch.enable", {"patterns": [{"urlPattern": "*", "requestStage": "Request"}]}, sid_b
        )

        await a.send("Page.enable", {}, sid_a)
        await a.send("Page.navigate", {"url": server.url + "/form"}, sid_a)
        for _ in range(100):
            r = await a.send("Runtime.evaluate", {"expression": "document.title"}, sid_a)
            if r["result"].get("value") == "form":
                break
            await asyncio.sleep(0.05)
        js = (
            "const f=document.createElement('form');f.method='POST';"
            f"f.action='{server.url}/post';const i=document.createElement('input');"
            "i.name='v';i.value='1';f.appendChild(i);document.body.appendChild(f);f.submit();1"
        )
        await a.send("Runtime.evaluate", {"expression": js}, sid_a)
        for _ in range(60):
            if any(r["method"] == "POST" for r in paused):
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)  # give a leaked request time to land

        print("spike chrome version:", version)
        assert any(r["method"] == "POST" and r["url"].endswith("/post") for r in paused)
        assert any(r["method"] == "GET" and r["url"].endswith("/form") for r in paused)
        assert server.hits == 0
        # client A is still fully usable after B blocked the request
        r = await a.send("Runtime.evaluate", {"expression": "1+1"}, sid_a)
        assert r["result"]["value"] == 2
    finally:
        for c in (a, b):
            if c:
                await c.close()
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(10)
        server.stop()
        shutil.rmtree(profile, ignore_errors=True)


# --- ActionGuard (swarm.browser.guard) ------------------------------------------------


def kinds(log, kind: str, order_id: str | None = None) -> list[dict]:
    return [
        e["payload"]
        for e in log.query(kinds=[kind])
        if order_id is None or e["payload"].get("order_id") == order_id
    ]


async def test_guard_blocks_post_without_token(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_g")
    assert s["guard"] == "off"
    await guard.arm(s, order_id="wo_1", mode="guarded", run_id="run_g")
    assert s["guard"] == "guarded"
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        seen = await h.submit_form(guard_server.url + "/post")
    assert guard_server.hits == 0
    assert "BLOCKED_BY_CLIENT" in seen.get("error", ""), seen
    blocked = kinds(log, "guard.blocked", "wo_1")
    assert len(blocked) == 1 and blocked[0]["reason"] == "needs_token"
    d = blocked[0]["descriptor"]
    assert (d["method"], d["host"], d["path"]) == ("POST", "127.0.0.1", "/post")
    assert d["body_sha256"] == hashlib.sha256(b"v=1").hexdigest()
    ev = next(log.query(kinds=["guard.blocked"]))
    assert ev["run_id"] == "run_g"


async def test_token_is_single_use_and_fingerprint_dedups(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_t")
    await guard.arm(s, order_id="wo_1", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        first = await h.fetch(guard_server.url + "/post", body="pay=1")
        assert "error" in first and guard_server.hits == 0
        desc = kinds(log, "guard.blocked", "wo_1")[-1]["descriptor"]
        token = ApprovalToken.from_descriptor("ask_1", desc, order_id="wo_1")
        guard.grant(token)
        ok = await h.fetch(guard_server.url + "/post", body="pay=1")
        assert ok == {"status": 200, "body": "posted ok"} and guard_server.hits == 1
        assert token.used
        executed = kinds(log, "guard.executed", "wo_1")
        assert len(executed) == 1 and executed[0]["fingerprint"] == desc["fingerprint"]
        assert executed[0]["ask_id"] == "ask_1"
        again = await h.fetch(guard_server.url + "/post", body="pay=1")
        assert again["status"] == 409 and "already executed" in again["body"]
        assert guard_server.hits == 1
        other_body = await h.fetch(guard_server.url + "/post", body="pay=2")
        assert "error" in other_body and guard_server.hits == 1  # token already consumed
    with pytest.raises(ValueError):
        guard.grant(token)

    # restart: a fresh guard on a fresh connection still knows what wo_1 executed
    fresh = await chrome.open_guard("test", allowlist=[])
    try:
        with pytest.raises(ValueError):  # one Ask = one token, also for a fresh guard
            fresh.grant(ApprovalToken.from_descriptor("ask_1", desc, order_id="wo_1"))
        s2 = await chrome.open_session(profile="test", purpose="run_t2")
        await fresh.arm(s2, order_id="wo_1", mode="guarded")
        s3 = await chrome.open_session(profile="test", purpose="run_t3")
        await fresh.arm(s3, order_id="wo_2", mode="guarded")
        async with Harness(s2) as h2, Harness(s3) as h3:
            await h2.goto(guard_server.url + "/form")
            await h3.goto(guard_server.url + "/form")
            dup = await h2.fetch(guard_server.url + "/post", body="pay=1")
            assert dup["status"] == 409 and "already executed" in dup["body"]
            other_order = await h3.fetch(guard_server.url + "/post", body="pay=1")
            assert "error" in other_order
        assert guard_server.hits == 1
        assert len(kinds(log, "guard.already_executed", "wo_1")) == 2
    finally:
        await fresh.close()


async def test_get_requests_pass_and_allowlist_post_passes(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_a")
    await guard.arm(s, order_id="wo_1", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")  # GET document passes
        got = await h.fetch(guard_server.url + "/post?q=1", method="GET")
        assert got["status"] == 200 and guard_server.hits == 1
        head = await h.fetch(guard_server.url + "/post", method="HEAD")
        assert head["status"] == 200 and guard_server.hits == 2
        allowed = await h.fetch(guard_server.local_url + "/post", body="x")  # localhost listed
        assert allowed == {"status": 200, "body": "posted ok"} and guard_server.hits == 3
        blocked = await h.fetch(guard_server.url + "/post", body="x")
        assert "error" in blocked and guard_server.hits == 3
    assert kinds(log, "guard.executed") == []  # allowlisted traffic is not "irreversible"


async def test_dry_run_blocks_allowlisted_post_and_never_grants(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_d")
    await guard.arm(s, order_id="wo_d", mode="dry_run")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        r = await h.fetch(guard_server.local_url + "/post", body="x")
        assert "error" in r
        blocked = kinds(log, "guard.blocked", "wo_d")[-1]["descriptor"]
        guard.grant(ApprovalToken.from_descriptor("ask_d", blocked, order_id="wo_d"))
        r = await h.fetch(guard_server.local_url + "/post", body="x")
        assert "error" in r
    assert guard_server.hits == 0
    assert {b["reason"] for b in kinds(log, "guard.blocked", "wo_d")} == {"dry_run"}


async def test_off_mode_passes_everything_and_set_mode_switches(chrome, guard, guard_server):
    s = await chrome.open_session(profile="test", purpose="run_o")
    await guard.arm(s, order_id="wo_o", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        await guard.set_mode(s["id"], "off")  # human takeover
        assert (await h.fetch(guard_server.url + "/post", body="x"))["status"] == 200
        await guard.set_mode(s["id"], "guarded")
        assert "error" in await h.fetch(guard_server.url + "/post", body="x")
    assert guard_server.hits == 1


async def test_popup_post_is_guarded(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_p")
    await guard.arm(s, order_id="wo_p", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        await h.submit_form(guard_server.url + "/post", target="_blank")
        await h.evaluate(f"window.open('{guard_server.url}/popup'); 1", gesture=True)
        for _ in range(60):  # the GET popup was resumed and loaded
            targets = (await h.client.send("Target.getTargets"))["targetInfos"]
            if any(t["url"].endswith("/popup") for t in targets):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("GET popup never loaded")
        noopener = (  # rel=noopener popups still report the guarded page as opener
            "(() => {const f=document.createElement('form');f.method='POST';f.target='_blank';"
            f"f.rel='noopener';f.action='{guard_server.url}/post2';document.body.appendChild(f);"
            "f.submit();return 1})()"
        )
        await h.evaluate(noopener, gesture=True)
        await asyncio.sleep(1.5)
    assert guard_server.hits == 0
    paths = [b["descriptor"]["path"] for b in kinds(log, "guard.blocked", "wo_p")]
    assert paths == ["/post", "/post2"]


async def test_unarmed_sessions_are_not_guarded_or_stalled(chrome, guard, guard_server):
    armed = await chrome.open_session(profile="test", purpose="run_x")
    await guard.arm(armed, order_id="wo_x", mode="guarded")
    free = await chrome.open_session(profile="test", purpose="run_y")  # created after arming
    async with Harness(free) as h:
        await h.goto(guard_server.url + "/form")
        assert (await h.fetch(guard_server.url + "/post", body="x"))["status"] == 200
    assert guard_server.hits == 1


async def test_guard_connection_loss_closes_armed_targets(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_l")
    free = await chrome.open_session(profile="test", purpose="run_free")
    await guard.arm(s, order_id="wo_l", mode="guarded")
    guard._client._ws.transport.abort()  # the guard's websocket dies unexpectedly
    probe = await chrome.connect_client("test")
    try:
        for _ in range(100):
            ids = {t["targetId"] for t in (await probe.send("Target.getTargets"))["targetInfos"]}
            if s["target_id"] not in ids:
                break
            await asyncio.sleep(0.05)
        assert s["target_id"] not in ids  # the unguarded tab cannot keep running
        assert free["target_id"] in ids  # other sessions are untouched
    finally:
        await probe.close()
    assert s["guard"] == "off"
    lost = kinds(log, "guard.disconnected")
    assert lost and lost[0]["targets"] == [s["target_id"]] and lost[0]["orders"] == ["wo_l"]
    assert kinds(log, "browser.fail_closed")[0]["killed"] is False


async def test_shared_worker_post_is_blocked(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_sw")
    await guard.arm(s, order_id="wo_sw", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        msg = await h.evaluate(
            "new Promise(res => { const w = new SharedWorker('/shared.js');"
            " w.port.onmessage = (e) => res(e.data); w.port.start();"
            " setTimeout(() => res('timeout'), 4000); })"
        )
    assert guard_server.hits == 0, msg
    assert msg.startswith("error"), msg
    blocked = kinds(log, "guard.blocked", "wo_sw")  # attributed: the only active arm
    assert [b["descriptor"]["path"] for b in blocked] == ["/post"]


async def test_dedicated_worker_post_is_blocked(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_dw")
    await guard.arm(s, order_id="wo_dw", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        msg = await h.evaluate(
            "new Promise(res => { const w = new Worker('/worker.js');"
            " w.onmessage = (e) => res(e.data); setTimeout(() => res('timeout'), 4000); })"
        )
    assert guard_server.hits == 0, msg
    assert msg.startswith("error"), msg
    assert [b["descriptor"]["path"] for b in kinds(log, "guard.blocked", "wo_dw")] == ["/post"]


async def test_redirected_post_is_re_evaluated(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_r")
    await guard.arm(s, order_id="wo_r", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        assert "error" in await h.fetch(guard_server.url + "/redirect", body="x")
        desc = kinds(log, "guard.blocked", "wo_r")[-1]["descriptor"]
        token = ApprovalToken.from_descriptor("ask_r", desc, order_id="wo_r")
        guard.grant(token)  # approves POST /redirect only
        r = await h.fetch(guard_server.url + "/redirect", body="x")
    assert "error" in r, r
    assert guard_server.redirect_hits == 1  # the approved hop went out
    assert guard_server.hits == 0  # the 307 hop to another path needed its own approval
    paths = [b["descriptor"]["path"] for b in kinds(log, "guard.blocked", "wo_r")]
    assert paths == ["/redirect", "/post-redirected"]


async def test_cross_site_iframe_post_is_blocked(chrome, guard, guard_server, log):
    free = await chrome.open_session(profile="test", purpose="run_f0")
    async with Harness(free) as h:  # control: unguarded, the frame really posts
        await h.goto(guard_server.url + "/framehost", title="framehost")
        for _ in range(100):
            if guard_server.hits:
                break
            await asyncio.sleep(0.05)
    assert guard_server.hits == 1
    s = await chrome.open_session(profile="test", purpose="run_f")
    await guard.arm(s, order_id="wo_f", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/framehost", title="framehost")
        for _ in range(60):
            if kinds(log, "guard.blocked", "wo_f"):
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.3)
    assert guard_server.hits == 1
    blocked = kinds(log, "guard.blocked", "wo_f")
    assert [(b["descriptor"]["host"], b["descriptor"]["path"]) for b in blocked] == [
        ("127.0.0.1", "/post")
    ]


SHARED_JS = (
    "onconnect = (e) => {{ const p = e.ports[0]; fetch('{url}/post', {{method: 'POST', "
    "body: 'from-worker'}}).then(r => p.postMessage('status ' + r.status), "
    "err => p.postMessage('error ' + err)); }};"
)


def connect_shared(src_expr: str) -> str:
    """JS: connect to a SharedWorker (src from `src_expr`), resolve with its first message."""
    return (
        "new Promise(res => { try { const w = new SharedWorker(" + src_expr + ");"
        " w.port.onmessage = (e) => res(e.data); w.port.start();"
        " setTimeout(() => res('timeout'), 4000); } catch (err) { res('ctor ' + err); } })"
    )


async def test_blob_and_data_shared_workers_are_guarded(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_b")
    await guard.arm(s, order_id="wo_b", mode="guarded")
    code = SHARED_JS.format(url=guard_server.url)
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        blob = await h.evaluate(
            connect_shared(
                f"URL.createObjectURL(new Blob([{json.dumps(code)}], {{type: 'text/javascript'}}))"
            )
        )
        data = await h.evaluate(
            connect_shared(json.dumps("data:text/javascript," + code.replace("%", "%25")))
        )
    print("blob worker:", blob, "| data worker:", data)
    assert guard_server.hits == 0, (blob, data)
    assert blob.startswith("error"), blob
    assert data.startswith("error"), data  # Chrome 156 allows data: SharedWorkers
    reasons = [b["descriptor"]["path"] for b in kinds(log, "guard.blocked", "wo_b")]
    assert reasons == ["/post", "/post"]  # both workers' POSTs were stopped by the guard


async def test_worker_stays_guarded_across_arms(chrome, guard, guard_server, log):
    a = await chrome.open_session(profile="test", purpose="run_a")
    b = await chrome.open_session(profile="test", purpose="run_b")
    await guard.arm(a, order_id="wo_a", mode="guarded")
    async with Harness(a) as ha:
        await ha.goto(guard_server.url + "/form")
        first = await ha.evaluate(connect_shared("'/shared.js'"))  # worker adopted under A
        await guard.arm(b, order_id="wo_b", mode="guarded")
        async with Harness(b) as hb:
            await hb.goto(guard_server.url + "/form")
            both = await hb.evaluate(connect_shared("'/shared.js'"))  # two arms active
            await guard.disarm(a["id"])
            only_b = await hb.evaluate(connect_shared("'/shared.js'"))  # B alone now
    assert guard_server.hits == 0, (first, both, only_b)
    assert all(m.startswith("error") for m in (first, both, only_b)), (first, both, only_b)
    by_order = [(b["order_id"], b["reason"]) for b in kinds(log, "guard.blocked")]
    assert by_order == [
        ("wo_a", "needs_token"),
        ("(unattributed)", "unattributed_worker"),
        ("wo_b", "needs_token"),
    ]


async def test_worker_started_before_any_arm_is_guarded_after_arm(chrome, guard, guard_server, log):
    free = await chrome.open_session(profile="test", purpose="run_free")
    async with Harness(free) as hf:
        await hf.goto(guard_server.url + "/form")
        before = await hf.evaluate(connect_shared("'/shared.js'"))
        assert before == "status 200" and guard_server.hits == 1
        s = await chrome.open_session(profile="test", purpose="run_g")
        await guard.arm(s, order_id="wo_g", mode="guarded")  # sweep picks the running worker
        async with Harness(s) as hg:
            await hg.goto(guard_server.url + "/form")
            after = await hg.evaluate(connect_shared("'/shared.js'"))
    assert after.startswith("error"), after
    assert guard_server.hits == 1
    closed = kinds(log, "guard.preexisting_worker_closed")
    assert [c["type"] for c in closed] == ["shared_worker"]


async def test_connection_loss_also_closes_guarded_workers(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_w")
    await guard.arm(s, order_id="wo_w", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        await h.evaluate(connect_shared("'/shared.js'"))
        guard._client._ws.transport.abort()
        for _ in range(100):
            if kinds(log, "browser.fail_closed"):
                break
            await asyncio.sleep(0.05)
    fc = kinds(log, "browser.fail_closed")[0]
    print("fail_closed:", fc)
    assert len(fc["workers"]) == 1
    probe = await chrome.connect_client("test")
    try:
        infos = (await probe.send("Target.getTargets"))["targetInfos"]
    finally:
        await probe.close()
    alive = {t["targetId"] for t in infos}
    assert s["target_id"] not in alive
    assert fc["workers"][0] not in alive or fc["workers_unclosed"] == fc["workers"]


async def test_service_worker_post_is_blocked(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_sv")
    await guard.arm(s, order_id="wo_sv", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        msg = await h.evaluate(
            "navigator.serviceWorker.register('/sw.js')"
            ".then(() => navigator.serviceWorker.ready)"
            ".then(reg => new Promise(res => {"
            " navigator.serviceWorker.onmessage = (e) => res(e.data);"
            " reg.active.postMessage('go'); setTimeout(() => res('timeout'), 5000); }))"
            ".catch(err => 'register ' + err)"
        )
    print("service worker:", msg)
    assert guard_server.hits == 0, msg
    assert msg.startswith("error"), msg
    blocked = kinds(log, "guard.blocked", "wo_sv")  # attributed: the only active arm
    assert [b["descriptor"]["path"] for b in blocked] == ["/post"]


async def test_takeover_tab_is_not_paused_and_resumes_guarding(chrome, guard, guard_server, log):
    s = await chrome.open_session(profile="test", purpose="run_t")
    await guard.arm(s, order_id="wo_t", mode="guarded")
    async with Harness(s) as h:
        await h.goto(guard_server.url + "/form")
        await guard.set_mode(s["id"], "off")
        assert (await h.fetch(guard_server.url + "/post", body="human"))["status"] == 200
        await guard.set_mode(s["id"], "guarded")
        assert "error" in await h.fetch(guard_server.url + "/post", body="bot")
    assert guard_server.hits == 1
    assert [b["descriptor"]["path"] for b in kinds(log, "guard.blocked", "wo_t")] == ["/post"]
