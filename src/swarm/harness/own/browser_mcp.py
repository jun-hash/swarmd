"""stdio MCP server that drives ONE Chrome tab over CDP (the worker's browser tools).

Spawned by `claude -p --mcp-config`. It never creates tabs: it attaches to `--target-id` and
navigates inside it. The ActionGuard lives in the daemon and intercepts our requests from there;
here we only *notice* a block (see `_Net`) and tell the model to ask for approval.

Block detection (deliberately simple): while an action runs we watch `Network.*` on our target.
A request that ends in `Network.loadingFailed` with errorText `net::ERR_BLOCKED_BY_CLIENT`
(what the guard's `Fetch.failRequest(BlockedByClient)` produces) during the action's settle
window is treated as "blocked by the guard". Any other failure is not.
"""

import argparse
import asyncio
import base64
import contextlib
import hashlib
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx
from mcp.server.mcpserver import MCPServer
from pydantic import TypeAdapter

from swarm.browser.cdp import CdpClient, CdpClosed, CdpError
from swarm.harness.own.observe import OBSERVERS
from swarm.types import AskDraft, Evidence

ASKS_PATH = "/internal/runs/{run}/asks"
EVIDENCE_PATH = "/internal/runs/{run}/evidence"
EXTRACT_LIMIT = 8_000
EXTRACT_JS = "function(){return this.innerText ?? this.value ?? this.textContent}"
SETTLE_MAX_S = 5.0
BLOCKED_MESSAGE = (
    "이 행동은 승인이 필요해 차단됐습니다. "
    "ask_human(kind='approval', irreversible=true)로 승인을 요청하세요."
)
ASK_SENT_PREFIX = "질문을 사람에게 보냈습니다"  # own harness: marks a parked ask
ASK_FAILED = "질문 전송 실패: 내부 API에 닿지 못했습니다 ({reason}). 잠시 후 다시 시도하세요."
STALE_REF = "ref {ref}를 찾을 수 없습니다. browser_snapshot을 다시 호출해 최신 ref를 받으세요."
SPILL_MISSING = "{key}: 해당 항목이 없습니다. 마지막 browser_snapshot에 나온 #s 번호만 조회됩니다."

_ASK_ADAPTER = TypeAdapter(AskDraft)
APPROVAL_OPTIONS = ["승인", "거절"]  # the only options of approval / permission Asks
_EVIDENCE_ADAPTER = TypeAdapter(Evidence)
GUARD_BLOCK_ERROR = "net::ERR_BLOCKED_BY_CLIENT"
TOOL_NAMES = (
    "browser_goto", "browser_snapshot", "browser_click", "browser_type", "browser_scroll",
    "browser_screenshot", "browser_extract", "browser_tabs", "browser_recall", "ask_human",
    "record_evidence",
)  # fmt: skip  # fixed tool surface; build_server registers exactly these


def _reason(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        return f"HTTP {e.response.status_code}"
    return type(e).__name__ + (f": {e}" if str(e) else "")


def _ask_draft(
    kind: str,
    question: str,
    options: list[str],
    recommended: str | None,
    default_after_minutes: int | None,
    irreversible: bool,
) -> AskDraft:
    """The Ask the model asked for, with approval / permission Asks forced into their one safe
    shape; `forced` names every field that had to change."""
    forced: list[str] = []
    if kind in ("approval", "permission"):
        # an approval unlocks a blocked request: never by default, and only the two fixed
        # options, so no "예/아니오" wording can invert what approving means
        if not irreversible:
            forced.append("irreversible")
        if default_after_minutes is not None:
            forced.append("default_after")
        if options != APPROVAL_OPTIONS:
            forced.append("options")
        if recommended is not None:
            forced.append("recommended")
        irreversible, options, recommended = True, list(APPROVAL_OPTIONS), None
    default_after = (
        None
        if irreversible or default_after_minutes is None
        else timedelta(minutes=default_after_minutes)
    )  # invariant: irreversible => no default
    return _ASK_ADAPTER.validate_python(
        {
            "kind": kind,
            "question": question,
            "options": options,
            "recommended": recommended,
            "default_after": default_after,
            "irreversible": irreversible,
            "evidence": [],
            **({"forced": forced} if forced else {}),
        }
    )


def _ask_reply(data: object) -> str:
    """The tool result for the internal API's answer to an Ask: the human's answer if the Ask
    is already closed, otherwise the "sent, end your turn" marker the harness parks on."""
    if not isinstance(data, dict) or not isinstance(data.get("ask_id"), str):
        return ASK_FAILED.format(reason="내부 API 응답 형식이 올바르지 않습니다")
    outcome = data.get("outcome")
    number = data.get("number")
    if outcome is not None:
        if not isinstance(outcome, dict):
            return ASK_FAILED.format(reason="내부 API 응답 형식이 올바르지 않습니다")
        choice = outcome.get("choice")
        if isinstance(choice, str) and choice:
            return choice
        raw = outcome.get("raw_text")
        if outcome.get("status") == "answered" and isinstance(raw, str) and raw:
            return raw
        return (
            f"질문이 닫혔지만 선택값이 없습니다(status={outcome.get('status')}). "
            "사람의 답이 없으니 되돌릴 수 없는 행동은 하지 마세요."
        )
    tag = f"(#{number})" if isinstance(number, int) and not isinstance(number, bool) else ""
    return f"{ASK_SENT_PREFIX}{tag}. 지금 턴을 끝내세요. 답은 다음 입력으로 옵니다."


class _Net:
    """Tracks in-flight requests and guard-blocked requests on our target."""

    def __init__(self) -> None:
        self.methods: dict[str, str] = {}
        self.inflight: set[str] = set()
        self.blocked: list[str] = []
        self.last_activity = time.monotonic()

    def on_request(self, p: dict, _sid: str | None) -> None:
        rid = p["requestId"]
        self.methods[rid] = p["request"].get("method", "GET")
        self.inflight.add(rid)
        self.last_activity = time.monotonic()

    def on_done(self, p: dict, _sid: str | None) -> None:
        self.inflight.discard(p["requestId"])
        self.methods.pop(p["requestId"], None)
        self.last_activity = time.monotonic()

    def on_failed(self, p: dict, _sid: str | None) -> None:
        rid = p["requestId"]
        self.inflight.discard(rid)
        self.methods.pop(rid, None)
        self.last_activity = time.monotonic()
        # guard.py blocks with Fetch.failRequest(errorReason="BlockedByClient"); that and only
        # that surfaces as net::ERR_BLOCKED_BY_CLIENT. CORS, DNS, offline etc. are not blocks.
        if p.get("errorText") == GUARD_BLOCK_ERROR:
            self.blocked.append(GUARD_BLOCK_ERROR)


class BrowserTools:
    def __init__(
        self,
        *,
        cdp_url: str,
        target_id: str,
        run_id: str,
        internal: str,
        token: str,
        blob_dir: Path,
        policy: Literal["naive", "compressed"],
    ) -> None:
        self.cdp_url, self.target_id, self.run_id = cdp_url, target_id, run_id
        self.internal, self.token, self.blob_dir, self.policy = internal, token, blob_dir, policy
        self.client: CdpClient | None = None
        self.sid = ""
        self.net = _Net()
        self.refs: dict[str, str] = {}
        self.spill: dict[str, str] = {}
        self._lock = asyncio.Lock()

    # --- connection ------------------------------------------------------------------

    async def _cdp(self) -> tuple[CdpClient, str]:
        if self.client is not None and self.client.connected:
            return self.client, self.sid
        client = CdpClient(self.cdp_url)
        await client.connect()
        sid = await client.attach(self.target_id)
        client.on("Network.requestWillBeSent", self.net.on_request)
        client.on("Network.loadingFinished", self.net.on_done)
        client.on("Network.loadingFailed", self.net.on_failed)
        for domain in ("Page", "Network", "DOM", "Runtime"):
            await client.send(f"{domain}.enable", {}, sid)
        self.client, self.sid = client, sid
        return client, sid

    async def _send(self, method: str, params: dict | None = None) -> dict:
        client, sid = await self._cdp()
        return await client.send(method, params, sid)

    async def _eval(self, expression: str):
        r = await self._send(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
        )
        return r.get("result", {}).get("value")

    async def _where(self) -> str:
        url = await self._eval("location.href")
        title = await self._eval("document.title")
        return f"url: {url}\ntitle: {title}"

    async def _settle(self) -> str:
        """Wait (<= SETTLE_MAX_S) for the page to go quiet; return a guard-block note if any."""
        deadline = time.monotonic() + SETTLE_MAX_S
        await asyncio.sleep(0.3)  # let a navigation/request start
        while time.monotonic() < deadline:
            quiet = time.monotonic() - self.net.last_activity
            if not self.net.inflight and quiet > 0.3:
                break
            await asyncio.sleep(0.1)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._wait_ready(), max(0.1, deadline - time.monotonic()))
        return self._take_block_note()

    async def _wait_ready(self) -> None:
        while await self._eval("document.readyState") != "complete":
            await asyncio.sleep(0.1)

    def _take_block_note(self) -> str:
        if not self.net.blocked:
            return ""
        self.net.blocked.clear()
        return BLOCKED_MESSAGE + "\n"

    async def _node_center(self, ref: str) -> tuple[float, float] | None:
        backend = self.refs.get(ref)
        if backend is None:
            return None
        node = {"backendNodeId": int(backend)}
        try:
            await self._send("DOM.scrollIntoViewIfNeeded", node)
            box = (await self._send("DOM.getBoxModel", node))["model"]["content"]
        except CdpError:
            return None
        return sum(box[0::2]) / 4, sum(box[1::2]) / 4

    # --- tools -----------------------------------------------------------------------

    async def goto(self, url: str) -> str:
        self.net.blocked.clear()
        r = await self._send("Page.navigate", {"url": url})
        if r.get("errorText"):
            return f"이동 실패: {r['errorText']}"
        note = await self._settle()
        return note + await self._where()

    async def snapshot(self) -> str:
        nodes = (await self._send("Accessibility.getFullAXTree"))["nodes"]
        spill: dict[str, str] = {}
        fn = OBSERVERS[self.policy]
        text, refs = fn(nodes, max_chars=EXTRACT_LIMIT, spill=spill)
        self.refs, self.spill = refs, spill  # valid until the next snapshot
        return f"{await self._where()}\n---\n{text}"

    async def click(self, ref: str) -> str:
        center = await self._node_center(ref)
        if center is None:
            return STALE_REF.format(ref=ref)
        x, y = center
        self.net.blocked.clear()
        base = {"x": x, "y": y, "button": "left"}
        await self._send("Input.dispatchMouseEvent", {"type": "mouseMoved", "x": x, "y": y})
        await self._send(
            "Input.dispatchMouseEvent", {**base, "type": "mousePressed", "clickCount": 1}
        )
        await self._send(
            "Input.dispatchMouseEvent", {**base, "type": "mouseReleased", "clickCount": 1}
        )
        note = await self._settle()
        return note + await self._where()

    async def type_text(self, ref: str, text: str, submit: bool) -> str:
        backend = self.refs.get(ref)
        if backend is None:
            return STALE_REF.format(ref=ref)
        try:
            await self._send("DOM.focus", {"backendNodeId": int(backend)})
        except CdpError:
            return STALE_REF.format(ref=ref)
        self.net.blocked.clear()
        await self._send("Input.insertText", {"text": text})
        if submit:
            key = {"key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13}
            await self._send("Input.dispatchKeyEvent", {**key, "type": "keyDown", "text": "\r"})
            await self._send("Input.dispatchKeyEvent", {**key, "type": "keyUp"})
            note = await self._settle()
            return note + await self._where()
        return "입력했습니다."

    async def scroll(self, direction: str, amount: int) -> str:
        dx, dy = {
            "down": (0, amount), "up": (0, -amount), "right": (amount, 0), "left": (-amount, 0),
        }.get(direction, (0, amount))  # fmt: skip
        pos = await self._eval(
            f"(window.scrollBy({dx},{dy}), `${{Math.round(scrollX)}},${{Math.round(scrollY)}}`)"
        )
        return f"scrolled {direction} {amount}px; scroll position x,y = {pos}"

    async def _capture(self) -> Path:
        r = await self._send("Page.captureScreenshot", {"format": "png"})
        path = self.blob_dir / f"shot_{int(time.time())}_{uuid.uuid4().hex[:8]}.png"
        path.write_bytes(base64.b64decode(r["data"]))
        return path

    async def screenshot(self) -> str:
        return f"스크린샷 저장: {await self._capture()}"

    async def extract(self, ref: str | None) -> str:
        if ref is None:
            text = await self._eval("document.body ? document.body.innerText : ''")
        else:
            backend = self.refs.get(ref)
            if backend is None:
                return STALE_REF.format(ref=ref)
            try:
                obj = (await self._send("DOM.resolveNode", {"backendNodeId": int(backend)}))[
                    "object"
                ]
                r = await self._send(
                    "Runtime.callFunctionOn",
                    {
                        "objectId": obj["objectId"],
                        "functionDeclaration": EXTRACT_JS,
                        "returnByValue": True,
                    },
                )
            except CdpError:
                return STALE_REF.format(ref=ref)
            text = r.get("result", {}).get("value")
        text = text or ""
        if len(text) > EXTRACT_LIMIT:
            return text[:EXTRACT_LIMIT] + f"\n…(잘림, 전체 {len(text)}자)"
        return text

    async def tabs(self) -> str:
        client, _ = await self._cdp()
        infos = (await client.send("Target.getTargets"))["targetInfos"]
        lines = [
            f"{'*' if i['targetId'] == self.target_id else '-'} [{i['targetId'][:8]}] "
            f"{i.get('title', '')} {i.get('url', '')}"
            for i in infos
            if i.get("type") == "page"
        ]
        return "\n".join(lines) + "\n(* = 이 도구가 조작하는 탭. 다른 탭은 조작할 수 없습니다.)"

    def recall(self, ref_id: str) -> str:
        return self.spill.get(ref_id) or SPILL_MISSING.format(key=ref_id)

    async def _post(self, path: str, body: bytes) -> dict:
        async with httpx.AsyncClient(timeout=30) as http:
            r = await http.post(
                self.internal + path,
                content=body,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                    # same request again (the model retrying after a timeout) -> same Ask
                    "Idempotency-Key": hashlib.sha256(path.encode() + b"\n" + body).hexdigest(),
                },
            )
            r.raise_for_status()
            return r.json()

    async def ask_human(
        self,
        kind: str,
        question: str,
        options: list[str],
        recommended: str | None,
        default_after_minutes: int | None,
        irreversible: bool,
    ) -> str:
        draft = _ask_draft(
            kind, question, options, recommended, default_after_minutes, irreversible
        )
        try:
            data = await self._post(
                ASKS_PATH.format(run=self.run_id), _ASK_ADAPTER.dump_json(draft)
            )
        except (httpx.HTTPError, ValueError) as e:
            return ASK_FAILED.format(reason=_reason(e))
        return _ask_reply(data)

    async def record_evidence(self, url: str, claim: str, screenshot: bool) -> str:
        path = await self._capture() if screenshot else None
        ev: Evidence = {
            "url": url,
            "screenshot": str(path) if path else None,
            "captured_at": datetime.now().astimezone(),
            "text": claim,  # the claim being proven
        }
        try:
            data = await self._post(
                EVIDENCE_PATH.format(run=self.run_id), _EVIDENCE_ADAPTER.dump_json(ev)
            )
            evidence_id = data["evidence_id"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as e:
            reason = _reason(e)
            return (
                f"증거 기록 실패: 내부 API에 닿지 못했습니다 ({reason}). 잠시 후 다시 시도하세요."
            )
        return f"증거를 기록했습니다({evidence_id})." + (f" 스크린샷: {path}" if path else "")

    async def close(self) -> None:
        if self.client is not None:
            await self.client.close()


def build_server(t: BrowserTools) -> MCPServer:
    mcp = MCPServer("swarmbrowser")

    async def guarded(coro):
        async with t._lock:  # one action at a time on one tab
            try:
                return await coro
            except (CdpClosed, ConnectionError) as e:
                return f"브라우저 연결이 끊겼습니다: {e}"
            except CdpError as e:
                return f"브라우저 오류: {e}"

    @mcp.tool()
    async def browser_goto(url: str) -> str:
        """Navigate this tab to a URL; returns the new URL and title."""
        return await guarded(t.goto(url))

    @mcp.tool()
    async def browser_snapshot() -> str:
        """Accessibility snapshot with @e<n> refs (valid until the next snapshot)."""
        return await guarded(t.snapshot())

    @mcp.tool()
    async def browser_click(ref: str) -> str:
        """Click the element with this @e<n> ref; returns the resulting URL and title."""
        return await guarded(t.click(ref))

    @mcp.tool()
    async def browser_type(ref: str, text: str, submit: bool = False) -> str:
        """Type text into the element with this ref; submit=true presses Enter."""
        return await guarded(t.type_text(ref, text, submit))

    @mcp.tool()
    async def browser_scroll(
        direction: Literal["up", "down", "left", "right"], amount: int = 800
    ) -> str:
        """Scroll the page by `amount` pixels."""
        return await guarded(t.scroll(direction, amount))

    @mcp.tool()
    async def browser_screenshot() -> str:
        """Save a PNG screenshot of the tab; returns its file path."""
        return await guarded(t.screenshot())

    @mcp.tool()
    async def browser_extract(ref: str | None = None) -> str:
        """Text of the element (or the whole page), truncated to 8,000 chars."""
        return await guarded(t.extract(ref))

    @mcp.tool()
    async def browser_tabs() -> str:
        """List open tabs and mark which one this tool drives."""
        return await guarded(t.tabs())

    @mcp.tool()
    async def browser_recall(ref_id: str) -> str:
        """Full text of a shortened item (#s<n>) from the last snapshot."""
        return t.recall(ref_id)

    @mcp.tool()
    async def ask_human(
        kind: Literal["decision", "approval", "permission", "takeover"],
        question: str,
        options: list[str],
        recommended: str | None = None,
        default_after_minutes: int | None = None,
        irreversible: bool = False,
    ) -> str:
        """Ask the human. Irreversible asks never get a default."""
        return await t.ask_human(
            kind, question, options, recommended, default_after_minutes, irreversible
        )

    @mcp.tool()
    async def record_evidence(url: str, claim: str, screenshot: bool = True) -> str:
        """Record evidence (URL, claim, optional screenshot) for the current run."""
        return await guarded(t.record_evidence(url, claim, screenshot))

    return mcp


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="swarm-browser-mcp")
    p.add_argument("--cdp-url", required=True)
    p.add_argument("--target-id", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--internal", required=True)
    p.add_argument("--token-file", required=True, type=Path)
    p.add_argument("--blob-dir", required=True, type=Path)
    p.add_argument("--policy", choices=["naive", "compressed"], default="compressed")
    a = p.parse_args(argv)
    a.blob_dir.mkdir(parents=True, exist_ok=True)
    tools = BrowserTools(
        cdp_url=a.cdp_url,
        target_id=a.target_id,
        run_id=a.run_id,
        internal=a.internal.rstrip("/"),
        token=a.token_file.read_text().strip(),
        blob_dir=a.blob_dir,
        policy=a.policy,
    )
    build_server(tools).run()


if __name__ == "__main__":
    main()
