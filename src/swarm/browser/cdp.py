"""Minimal Chrome DevTools Protocol client over one websocket.

One reader task owns the socket: replies are matched to `send` calls by id, events go to
callbacks registered with `on`. Flatten sessions are addressed with `session_id`
(the `sessionId` field of CDP messages).
"""

import asyncio
import inspect
import itertools
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

log = logging.getLogger(__name__)

EventCallback = Callable[[dict, str | None], Awaitable[None] | None]
CloseCallback = Callable[[], Awaitable[None] | None]


class CdpError(Exception):
    """CDP answered a command with an error object."""

    def __init__(self, method: str, code: int, message: str) -> None:
        super().__init__(f"{method}: {message} ({code})")
        self.method = method
        self.code = code
        self.message = message


class CdpClosed(ConnectionError):
    """The websocket is closed (by us or by the browser)."""


class CdpClient:
    def __init__(self, ws_url: str, *, timeout_s: float = 30.0) -> None:
        self.ws_url = ws_url
        self.timeout_s = timeout_s
        self._ws: ClientConnection | None = None
        self._reader: asyncio.Task | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, tuple[str, asyncio.Future]] = {}
        self._handlers: dict[str, list[EventCallback]] = {}
        self._close_cbs: list[CloseCallback] = []
        self._close_fired = False
        self._tasks: set[asyncio.Task] = set()
        self._closed = False

    async def connect(self) -> "CdpClient":
        self._ws = await connect(self.ws_url, max_size=None, ping_interval=None, open_timeout=10)
        self._reader = asyncio.create_task(self._read(self._ws), name=f"cdp-reader {self.ws_url}")
        return self

    async def __aenter__(self) -> "CdpClient":
        return await self.connect()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def connected(self) -> bool:
        return self._ws is not None and not self._closed

    async def send(
        self,
        method: str,
        params: dict | None = None,
        session_id: str | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict:
        if not self.connected:
            raise CdpClosed(f"cdp connection closed: {self.ws_url}")
        mid = next(self._ids)
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[mid] = (method, fut)
        msg: dict[str, Any] = {"id": mid, "method": method, "params": params or {}}
        if session_id:
            msg["sessionId"] = session_id
        try:
            assert self._ws is not None
            await self._ws.send(json.dumps(msg))
            return await asyncio.wait_for(fut, self.timeout_s if timeout_s is None else timeout_s)
        except ConnectionClosed as e:
            raise CdpClosed(str(e)) from e
        finally:
            self._pending.pop(mid, None)

    def on(self, event: str, cb: EventCallback) -> Callable[[], None]:
        """Register `cb(params, session_id)`; sync or async. Returns an unsubscribe function."""
        self._handlers.setdefault(event, []).append(cb)

        def off() -> None:
            cbs = self._handlers.get(event, [])
            if cb in cbs:
                cbs.remove(cb)

        return off

    def on_close(self, cb: CloseCallback) -> None:
        """Call `cb()` once when the connection ends, whether we closed it or the peer did."""
        self._close_cbs.append(cb)

    async def attach(self, target_id: str) -> str:
        """Attach to a target in flatten mode; returns the session id for `send(session_id=)`."""
        r = await self.send("Target.attachToTarget", {"targetId": target_id, "flatten": True})
        return r["sessionId"]

    async def close(self) -> None:
        self._closed = True
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # already gone
                pass
        if self._reader is not None and self._reader is not asyncio.current_task():
            try:
                await asyncio.wait_for(self._reader, 5)
            except (TimeoutError, asyncio.CancelledError):
                self._reader.cancel()
        self._fail_pending()

    def abort(self) -> None:
        """Drop the socket at once: no close handshake, nothing awaited. Pending sends fail
        with `CdpClosed` and the close callbacks fire now (once)."""
        self._closed = True
        if self._ws is not None:
            transport = getattr(self._ws, "transport", None)
            if transport is not None:
                try:
                    transport.abort()
                except Exception:  # already gone
                    pass
        self._fail_pending()
        self._fire_close()

    # --- internals ---------------------------------------------------------------------

    async def _read(self, ws: ClientConnection) -> None:
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    log.warning("cdp: non-json frame dropped")
                    continue
                if not isinstance(msg, dict):
                    log.warning("cdp: non-object frame dropped: %.80r", msg)
                    continue
                if "id" in msg:
                    self._resolve(msg)
                elif "method" in msg:
                    self._dispatch(msg["method"], msg.get("params") or {}, msg.get("sessionId"))
        except ConnectionClosed:
            pass
        finally:
            self._closed = True
            self._fail_pending()
            self._fire_close()

    def _resolve(self, msg: dict) -> None:
        entry = self._pending.get(msg["id"])
        if entry is None:
            return  # caller timed out
        method, fut = entry
        if fut.done():
            return
        if "error" in msg:
            err = msg["error"]
            fut.set_exception(CdpError(method, err.get("code", 0), err.get("message", "")))
        else:
            fut.set_result(msg.get("result") or {})

    def _dispatch(self, method: str, params: dict, session_id: str | None) -> None:
        for cb in list(self._handlers.get(method, [])):
            try:
                res = cb(params, session_id)
            except Exception:
                log.exception("cdp: callback for %s failed", method)
                continue
            if inspect.isawaitable(res):
                task = asyncio.ensure_future(res)
                self._tasks.add(task)
                task.add_done_callback(self._task_done)

    def _fire_close(self) -> None:
        if self._close_fired:
            return
        self._close_fired = True
        for cb in self._close_cbs:
            try:
                res = cb()
            except Exception:
                log.exception("cdp: close callback failed")
                continue
            if inspect.isawaitable(res):
                task = asyncio.ensure_future(res)
                self._tasks.add(task)
                task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("cdp: async callback failed", exc_info=task.exception())

    def _fail_pending(self) -> None:
        for _method, fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(CdpClosed(f"cdp connection closed: {self.ws_url}"))
        self._pending.clear()
