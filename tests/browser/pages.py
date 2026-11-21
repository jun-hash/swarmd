"""Harness-role helpers: a separate CDP connection drives the page (client A in the spike)."""

import asyncio
import json

from swarm.browser.cdp import CdpClient
from swarm.types import CdpSession


async def wait_title(client: CdpClient, sid: str, title: str, timeout_s: float = 5) -> None:
    for _ in range(int(timeout_s / 0.05)):
        r = await client.send("Runtime.evaluate", {"expression": "document.title"}, sid)
        if r["result"].get("value") == title:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"page title never became {title!r}")


class Harness:
    """One harness connection attached to the session's target."""

    def __init__(self, session: CdpSession) -> None:
        self.session = session
        self.client = CdpClient(session["cdp_url"])
        self.sid = ""

    async def __aenter__(self) -> "Harness":
        await self.client.connect()
        self.sid = await self.client.attach(self.session["target_id"])
        await self.client.send("Network.enable", {}, self.sid)
        return self

    async def __aexit__(self, *exc) -> None:
        await self.client.close()

    async def goto(self, url: str, title: str = "form") -> None:
        await self.client.send("Page.navigate", {"url": url}, self.sid)
        await wait_title(self.client, self.sid, title)

    async def evaluate(self, js: str, *, gesture: bool = False):
        r = await self.client.send(
            "Runtime.evaluate",
            {"expression": js, "awaitPromise": True, "returnByValue": True, "userGesture": gesture},
            self.sid,
        )
        return r["result"].get("value")

    async def submit_form(self, action: str, value: str = "1", *, target: str = "") -> dict:
        """POST a form from the current page; return what the network layer saw for it."""
        outcome: asyncio.Future = asyncio.get_running_loop().create_future()
        ids: set[str] = set()

        def on_request(p, sid):
            if p["request"]["url"] == action:
                ids.add(p["requestId"])

        def on_response(p, sid):
            if p["requestId"] in ids and not outcome.done():
                outcome.set_result({"status": p["response"]["status"]})

        def on_failed(p, sid):
            if p["requestId"] in ids and not outcome.done():
                outcome.set_result({"error": p["errorText"]})

        offs = [
            self.client.on("Network.requestWillBeSent", on_request),
            self.client.on("Network.responseReceived", on_response),
            self.client.on("Network.loadingFailed", on_failed),
        ]
        js = (
            "(() => {const f=document.createElement('form');f.method='POST';"
            f"f.action={json.dumps(action)};f.target={json.dumps(target)};"
            "const i=document.createElement('input');i.name='v';"
            f"i.value={json.dumps(value)};f.appendChild(i);document.body.appendChild(f);"
            "f.submit();return 1})()"
        )
        try:
            await self.evaluate(js, gesture=True)
            if target:  # popup: this connection cannot see the new target's network
                await asyncio.sleep(1.5)
                return {"popup": True}
            return await asyncio.wait_for(outcome, 5)
        finally:
            for off in offs:
                off()

    async def fetch(self, url: str, method: str = "POST", body: str = "x") -> dict:
        js = (
            f"fetch({json.dumps(url)}, {{method: {json.dumps(method)}, body: "
            f"{json.dumps(body) if method not in ('GET', 'HEAD') else 'undefined'}}})"
            ".then(r => r.text().then(t => ({status: r.status, body: t})))"
            ".catch(e => ({error: String(e)}))"
        )
        return await self.evaluate(js)
