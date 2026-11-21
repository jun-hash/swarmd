"""CdpClient against a scripted local websocket server (no Chrome)."""

import asyncio
import contextlib
import json

import pytest
from websockets.asyncio.server import serve

from swarm.browser.cdp import CdpClient, CdpClosed, CdpError


@pytest.fixture
async def fake_ws():
    """Scripted CDP peer. `script(ws)` coroutine drives each connection; `received` logs msgs."""

    class Peer:
        def __init__(self) -> None:
            self.script = None
            self.received: list[dict] = []
            self.url = ""

    peer = Peer()

    async def handler(ws):
        await peer.script(ws, peer)

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        peer.url = f"ws://127.0.0.1:{port}/devtools/browser/fake"
        yield peer


async def recv(ws, peer) -> dict:
    msg = json.loads(await ws.recv())
    peer.received.append(msg)
    return msg


async def test_cdp_send_matches_ids(fake_ws):
    async def script(ws, peer):
        first = await recv(ws, peer)
        second = await recv(ws, peer)
        # event (flatten session) before any reply, then replies out of order
        await ws.send(
            json.dumps({"method": "Page.loadEventFired", "params": {"t": 1}, "sessionId": "S1"})
        )
        await ws.send(json.dumps({"id": second["id"], "result": {"who": "second"}}))
        await ws.send(json.dumps({"id": first["id"], "result": {"who": "first"}}))
        await ws.wait_closed()

    fake_ws.script = script
    seen: list[tuple[dict, str | None]] = []
    client = await CdpClient(fake_ws.url).connect()
    client.on("Page.loadEventFired", lambda params, sid: seen.append((params, sid)))
    a, b = await asyncio.gather(
        client.send("Runtime.evaluate", {"expression": "1"}, session_id="S1"),
        client.send("Browser.getVersion"),
    )
    assert a == {"who": "first"} and b == {"who": "second"}
    assert seen == [({"t": 1}, "S1")]
    first, second = fake_ws.received
    assert first["sessionId"] == "S1" and "sessionId" not in second
    assert first["id"] != second["id"]
    await client.close()
    assert not client.connected


async def test_async_callbacks_run_and_errors_do_not_kill_reader(fake_ws):
    async def script(ws, peer):
        await ws.send(json.dumps({"method": "X.boom", "params": {}}))
        await ws.send(json.dumps({"method": "X.ev", "params": {"n": 2}}))
        msg = await recv(ws, peer)
        await ws.send(json.dumps({"id": msg["id"], "result": {}}))
        await ws.wait_closed()

    fake_ws.script = script
    got = asyncio.Event()

    def boom(params, sid):
        raise RuntimeError("callback bug")

    async def ok(params, sid):
        await asyncio.sleep(0)
        got.set()

    client = await CdpClient(fake_ws.url).connect()
    client.on("X.boom", boom)
    client.on("X.ev", ok)
    await asyncio.wait_for(got.wait(), 2)
    assert await client.send("Y.ping") == {}
    await client.close()


async def test_error_reply_raises_cdp_error(fake_ws):
    async def script(ws, peer):
        msg = await recv(ws, peer)
        await ws.send(json.dumps({"id": msg["id"], "error": {"code": -32000, "message": "nope"}}))
        await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    with pytest.raises(CdpError) as ei:
        await client.send("Target.closeTarget", {"targetId": "x"})
    assert ei.value.code == -32000 and "nope" in str(ei.value)
    await client.close()


async def test_attach_uses_flatten_and_returns_session_id(fake_ws):
    async def script(ws, peer):
        msg = await recv(ws, peer)
        await ws.send(json.dumps({"id": msg["id"], "result": {"sessionId": "FLAT1"}}))
        await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    assert await client.attach("T1") == "FLAT1"
    msg = fake_ws.received[0]
    assert msg["method"] == "Target.attachToTarget"
    assert msg["params"] == {"targetId": "T1", "flatten": True}
    await client.close()


async def test_peer_drop_fails_pending_and_later_sends(fake_ws):
    async def script(ws, peer):
        await recv(ws, peer)
        await ws.close()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    with pytest.raises(CdpClosed):
        await client.send("Never.answered")
    assert not client.connected
    with pytest.raises(CdpClosed):
        await client.send("After.close")
    await client.close()  # idempotent, no raise


async def test_close_fails_pending_send(fake_ws):
    async def script(ws, peer):
        await recv(ws, peer)
        await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    pending = asyncio.create_task(client.send("Slow.call"))
    await asyncio.sleep(0.05)
    await client.close()
    with pytest.raises(CdpClosed):
        await pending


async def test_non_object_frames_are_dropped_not_fatal(fake_ws):
    async def script(ws, peer):
        await ws.send("[1, 2]")
        await ws.send("5")
        await ws.send("not json")
        msg = await recv(ws, peer)
        await ws.send(json.dumps({"id": msg["id"], "result": {"ok": True}}))
        await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    assert await client.send("Still.alive") == {"ok": True}
    await client.close()


async def test_zero_timeout_is_not_the_default(fake_ws):
    async def script(ws, peer):
        await recv(ws, peer)
        await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url, timeout_s=30).connect()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.send("Never.answered", timeout_s=0), 2)
    await client.close()


async def test_on_close_fires_once_on_peer_drop_and_on_close(fake_ws):
    async def script(ws, peer):
        await ws.close()

    fake_ws.script = script
    fired: list[str] = []
    done = asyncio.Event()

    async def cb():
        fired.append("async")
        done.set()

    client = CdpClient(fake_ws.url)
    client.on_close(lambda: fired.append("sync"))
    client.on_close(cb)
    await client.connect()
    await asyncio.wait_for(done.wait(), 2)
    await client.close()
    await asyncio.sleep(0.05)
    assert fired == ["sync", "async"]


async def test_abort_drops_the_socket_without_a_close_handshake(fake_ws):
    async def script(ws, peer):
        await recv(ws, peer)  # never answered
        with contextlib.suppress(Exception):
            await ws.wait_closed()

    fake_ws.script = script
    client = await CdpClient(fake_ws.url).connect()
    fired: list[str] = []
    client.on_close(lambda: fired.append("closed"))
    pending = asyncio.ensure_future(client.send("Slow.command"))
    await asyncio.sleep(0.05)
    client.abort()  # synchronous: no handshake, no waiting on the peer
    assert client.connected is False
    with pytest.raises(CdpClosed):
        await pending
    with pytest.raises(CdpClosed):
        await client.send("After.abort")
    await asyncio.sleep(0.05)
    assert fired == ["closed"]
    await client.close()  # idempotent
    assert fired == ["closed"]
