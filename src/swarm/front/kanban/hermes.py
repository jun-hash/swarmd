"""HermesTransport: every Hermes kanban URL, header and payload shape (REST and WS), the
remote task / comment types and the errors a request can raise.

A new card is `POST .../tasks?board=<slug>` with `Idempotency-Key: <card key>`, a known one
`PATCH .../tasks/<id>?board=<slug>`; the board is read with `GET .../board` and followed over
WS `.../events?since=<id>`. Auth of the plugin routes and the exact shapes are still open
and were not probed live, so the parsers accept every shape seen so far. The
optional session token goes only into a header and is registered with `redact`, which keeps
it out of httpx / websockets logs and out of every error text built here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import quote, urlencode

import httpx

from swarm.redact import add_secret, redact

TOKEN_HEADER = "X-Hermes-Session-Token"
PLUGIN_PATH = "/api/plugins/kanban"
UNAVAILABLE_STATUS = frozenset({408, 425, 429})  # plus every 5xx: retry later
AUTH_STATUS = frozenset({401, 403})  # retried with backoff too, but alerted once
ERROR_BODY_MAX = 300  # a Hermes error body in an exception


class HermesUnavailable(Exception):
    """Dashboard down or busy (transport error, 5xx, 401/403/408/425/429): retry later."""


class HermesRejected(Exception):
    """Hermes refused this request (4xx): retrying the same payload cannot help."""

    def __init__(self, status: int | None, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


class HermesAuthFailed(HermesUnavailable):
    """401/403: the plugin routes want a (new) session token. Retried; alerted once."""


class HermesNotFound(HermesRejected):
    """404: the task is gone from the board."""


@dataclass(frozen=True)
class RemoteTask:
    """The part of a Hermes task the mirror reads."""

    id: str
    status: str | None
    priority: int | None
    updated_at: str | None


@dataclass(frozen=True)
class RemoteComment:
    """A comment created on a Hermes task."""

    id: str
    task_id: str
    body: str


class WsConn(Protocol):
    """An open events socket."""

    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


WsConnect = Callable[[str, dict[str, str]], Awaitable[WsConn]]


async def _websockets_connect(url: str, headers: dict[str, str]) -> WsConn:
    from websockets.asyncio.client import connect

    return await connect(url, additional_headers=headers, open_timeout=10, max_size=2**22)


def _int(v) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int | float):
        return int(v)
    if isinstance(v, str) and v.lstrip("-").isdigit():
        return int(v)
    return None


def _task(data, status: str | None = None) -> RemoteTask:
    t = data.get("task", data) if isinstance(data, dict) else None
    if not isinstance(t, dict) or t.get("id") is None:
        raise HermesRejected(None, "reply carries no task id")
    st = t.get("status", status)
    up = t.get("updated_at")
    return RemoteTask(
        id=str(t["id"]),
        status=str(st) if st is not None else None,
        priority=_int(t.get("priority")),
        updated_at=str(up) if up is not None else None,
    )


def _board_tasks(data) -> list[RemoteTask]:
    """GET .../board: a list, {"tasks": [...]} or {"columns": [{"status", "tasks"}]}."""
    if isinstance(data, list):
        pairs = [(t, None) for t in data]
    elif isinstance(data, dict) and isinstance(data.get("tasks"), list):
        pairs = [(t, None) for t in data["tasks"]]
    elif isinstance(data, dict) and isinstance(data.get("columns"), list):
        pairs = [
            (t, col.get("status") or col.get("id"))
            for col in data["columns"]
            if isinstance(col, dict)
            for t in col.get("tasks") or []
        ]
    else:
        raise HermesRejected(None, "board reply has no tasks")
    return [_task(t, st) for t, st in pairs if isinstance(t, dict) and t.get("id") is not None]


def parse_event(msg: dict) -> tuple[str | None, RemoteTask | RemoteComment | None]:
    """One WS event -> (event id for `since`, task change | comment | None)."""
    eid = event_id(msg)
    typ = str(msg.get("type") or msg.get("event") or "")
    c = msg.get("comment")
    task = msg.get("task") if isinstance(msg.get("task"), dict) else None
    if isinstance(c, dict) or typ.startswith("comment"):
        if not _is_creation(typ):
            return eid, None  # an edit / delete is not a new thing the human said
        c = c if isinstance(c, dict) else {}
        task_id = msg.get("task_id") or c.get("task_id") or (task or {}).get("id")
        if task_id is None:
            return eid, None
        body = c.get("body") or c.get("text") or ""
        return eid, RemoteComment(id=str(c.get("id") or ""), task_id=str(task_id), body=body)
    if task is not None and task.get("id") is not None:
        return eid, _task(task)
    return eid, None


def _is_creation(typ: str) -> bool:
    """Only comment-creation events count ("comment.created", "comment_added", ...)."""
    t = typ.lower()
    return t in ("comment", "comment.new") or t.endswith(("created", "added"))


def _sid(v) -> str | None:
    return None if v is None else str(v)


def event_id(msg: dict) -> str | None:
    """A WS event's id (or seq), the value `since` resumes after."""
    return _sid(msg.get("id", msg.get("seq")))


class HermesTransport:
    """REST and WS access to the Hermes dashboard's kanban plugin. Errors come out as
    HermesUnavailable (retry later) or HermesRejected (this payload cannot succeed)."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        token_header: str = TOKEN_HEADER,
        transport: httpx.AsyncBaseTransport | None = None,
        ws_connect: WsConnect | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._token = token or None
        self._headers = {token_header: self._token} if self._token else {}
        self._http = httpx.AsyncClient(
            base_url=self._base, transport=transport, timeout=timeout_s, headers=self._headers
        )
        self._ws_connect = ws_connect or _websockets_connect
        if self._token:
            add_secret(self._token)

    def __repr__(self) -> str:
        return f"HermesTransport({self._base!r}, token={'set' if self._token else 'none'})"

    async def _request(
        self, method: str, path: str, *, board: str, body: dict | None = None,
        headers: dict | None = None,
    ):  # fmt: skip
        try:
            resp = await self._http.request(
                method, PLUGIN_PATH + path, params={"board": board}, json=body, headers=headers
            )
        except httpx.HTTPError as e:
            raise HermesUnavailable(f"{type(e).__name__}: {redact(str(e))}") from None
        code = resp.status_code
        if code in AUTH_STATUS:
            raise HermesAuthFailed(f"HTTP {code}")
        if code >= 500 or code in UNAVAILABLE_STATUS:
            raise HermesUnavailable(f"HTTP {code}")
        if code == 404:
            raise HermesNotFound(404, redact(resp.text[:ERROR_BODY_MAX]))
        if code >= 400:
            raise HermesRejected(code, redact(resp.text[:ERROR_BODY_MAX]))
        try:
            return resp.json()
        except ValueError:
            return {}

    async def create_task(self, board: str, key: str, fields: dict) -> RemoteTask:
        data = await self._request(
            "POST", "/tasks", board=board, body=fields, headers={"Idempotency-Key": key}
        )
        return _task(data, fields.get("status"))

    async def update_task(self, board: str, task_id: str, fields: dict) -> RemoteTask:
        data = await self._request("PATCH", f"/tasks/{quote(task_id, safe='')}", board=board,
                                   body=fields)  # fmt: skip
        try:
            return _task(data, fields.get("status"))
        except HermesRejected:  # a PATCH reply without the task: we know the id anyway
            return RemoteTask(task_id, fields.get("status"), fields.get("priority"), None)

    async def add_comment(self, board: str, task_id: str, text: str) -> str:
        data = await self._request(
            "POST", f"/tasks/{quote(task_id, safe='')}/comments", board=board, body={"body": text}
        )
        c = data.get("comment", data) if isinstance(data, dict) else {}
        return str(c.get("id") or "") if isinstance(c, dict) else ""

    async def list_tasks(self, board: str) -> list[RemoteTask]:
        return _board_tasks(await self._request("GET", "/board", board=board))

    def events_url(self, since: str | None) -> str:
        scheme, _, rest = self._base.partition("://")
        ws = ("wss" if scheme == "https" else "ws") + "://" + rest
        query = "?" + urlencode({"since": since}) if since else ""
        return f"{ws}{PLUGIN_PATH}/events{query}"

    async def connect_events(self, since: str | None) -> WsConn:
        try:
            return await self._ws_connect(self.events_url(since), dict(self._headers))
        except Exception as e:  # noqa: BLE001 - any connect failure means "not now"
            raise HermesUnavailable(f"{type(e).__name__}: {redact(str(e))}") from None

    async def aclose(self) -> None:
        await self._http.aclose()
