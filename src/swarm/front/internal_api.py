"""Internal HTTP API for worker runs + the shared Starlette app (with SwarmFront MCP at /mcp).

Routes (the client is `harness/own/browser_mcp.py`, which pins the paths and bodies):

- `POST /internal/runs/{run_id}/asks`  body = AskDraft JSON (pydantic: timedelta "PT30M")
  -> `pool.raise_ask(run_id, draft)` (never the inbox directly: approval Asks must bind to
  the request the ActionGuard blocked) -> `{"ask_id", "outcome", "number"}`. A new open Ask
  is announced through the Notifier (`kind="ask"`, ref_id = ask_id). Optional header
  `Idempotency-Key`: a retry with the same key while that Ask is still open returns the same
  ticket instead of raising a second Ask.
- `POST /internal/runs/{run_id}/evidence`  body = Evidence JSON {url, screenshot,
  captured_at, text} ({claim, screenshot_path, ts} is accepted too)
  -> `pool.add_evidence` -> `{"evidence_id"}`.
- `POST /internal/runs/{run_id}/cancel`  body = {"reason": str} -> `pool.cancel(run_id,
  reason=...)` -> 200 `{"ok", "run_id"}` (404 unknown run); a cancel still cleaning up after
  `CANCEL_WAIT_S` answers 202 and finishes in the background. The watchdog's stuck-run
  cancel (reason "stuck") uses it.
- `GET /internal/health` -> `{"ok": true}` (no bearer; still loopback only).

An Ask that never reaches Telegram is never marked sent, so its deadline never starts and
its run waits forever. Hence: the announce falls back to General when the project channel
can't be used, and `sweep()` (at startup with no grace, then every 60 s via `run_sweeps`)
announces every open Ask the outbox has never seen (`notifier.has_ask`), healing a failed
announce or a crash between raise and enqueue. Ref-id dedupe in the outbox makes it safe
next to the Coordinator, which enqueues its own confirmation Asks.

Every other path (internal routes and /mcp) needs `Authorization: Bearer <secret>`
(constant-time compare), and any client that is not on loopback is refused. `make_server`
binds 127.0.0.1 only; there is no host parameter.
"""

import asyncio
import contextlib
import hmac
import ipaddress
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Protocol

import uvicorn
from pydantic import TypeAdapter, ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

from swarm.aio import pause
from swarm.clock import Clock, zone_of
from swarm.ids import new_id
from swarm.projects.store import ProjectStore
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.text import error_text
from swarm.types import (
    AskDraft,
    AskTicket,
    AskView,
    ChannelRef,
    Evidence,
    Outbound,
    RunId,
)

log = logging.getLogger(__name__)

LOOPBACK = "127.0.0.1"
IDEMPOTENCY_HEADER = "Idempotency-Key"
KEY_MAX = 200
OPEN_PATHS = frozenset({"/internal/health"})
TEXT_MAX = 3_500
QUESTION_MAX = 600
SWEEP_EVERY_S = 60.0
SWEEP_GRACE = timedelta(seconds=60)  # a just-raised Ask is being announced by its raiser
CANCEL_WAIT_S = 10.0  # bound on a cancel; a longer cleanup continues in the background
ERROR_MAX = 300  # an error text in an HTTP response or an event
REASON_MAX = 200

_ASK = TypeAdapter(AskDraft)
_EVIDENCE = TypeAdapter(Evidence)

_SCHEMA = """
CREATE TABLE front_ask_keys (
  run_id TEXT NOT NULL, key TEXT NOT NULL, ask_id TEXT NOT NULL, ticket_json TEXT NOT NULL,
  created_at TEXT NOT NULL, PRIMARY KEY (run_id, key))
"""

KIND_LABEL = {
    "decision": "🙋 결정",
    "approval": "🔐 승인",
    "permission": "🔑 권한",
    "takeover": "🖐 직접 처리",
}


class WorkerEndpoints(Protocol):
    """The slice of `WorkerPool` a running worker reaches through this API: raise an Ask,
    post evidence, cancel its own run. Its siblings are `front.kanban.RunReader` (the
    board's run reads) and the Coordinator's `RunControl`; `WorkerPool` satisfies all three."""

    async def raise_ask(self, run_id: RunId, draft: AskDraft) -> AskTicket: ...
    def add_evidence(self, run_id: RunId, ev: Evidence) -> None: ...
    async def cancel(self, run_id: RunId, *, reason: str) -> None: ...


class AskNotifier(Protocol):
    """The Notifier port plus the outbox lookup the announce sweep needs."""

    async def send(self, msg: Outbound) -> None: ...
    def has_ask(self, ask_id: str) -> bool: ...


class InboxPort(Protocol):
    def open_asks(self, project_id=None) -> list[AskView]: ...
    def topic_number(self, ask_id: str) -> int: ...


def _human_td(td: timedelta) -> str:
    mins = max(int(td.total_seconds() // 60), 1)
    h, m = divmod(mins, 60)
    if not h:
        return f"{m}분"
    return f"{h}시간" + (f" {m}분" if m else "")


def is_blocked(view: AskView) -> bool:
    """No default to fall back on: the work waits for a human (urgent, "멈춰 있어요")."""
    return view.get("default_after") is None or not view["recommended"]


def render_ask(number: int, view: AskView) -> str:
    """The Telegram text of an Ask: first line = type + the question."""
    kind = view["kind"]
    question = " ".join(view["question"].split())[:QUESTION_MAX]
    lines = [f"{KIND_LABEL.get(kind, '🙋 질문')} · #{number} {question}"]
    options, rec = view["options"], view["recommended"]
    if kind == "takeover":
        lines.append("맥미니 브라우저에서 직접 처리한 뒤 '됐어'라고 알려 주세요.")
    elif options:
        lines.append("선택지: " + " / ".join(options) + (f" (추천: {rec})" if rec else ""))
    after = view.get("default_after")
    if is_blocked(view) or after is None:
        lines.append("답을 주실 때까지 이 작업은 멈춰 있어요.")
    else:
        lines.append(f"{_human_td(after)} 안에 답이 없으면 {rec}로 진행해요.")
    lines.append(f"답: '#{number} <선택>'")
    return "\n".join(lines)[:TEXT_MAX]


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status)


class InternalApi:
    def __init__(
        self,
        *,
        db: Database,
        pool: WorkerEndpoints,
        inbox: InboxPort,
        projects: ProjectStore,
        notifier: AskNotifier,
        clock: Clock,
        general_channel: ChannelRef,
        log: EventLog | None = None,
    ) -> None:
        self._db = db
        self._pool = pool
        self._inbox = inbox
        self._projects = projects
        self._notifier = notifier
        self._clock = clock
        self._general = general_channel
        self._emitter = Emitter(log, clock, "front")
        self._inflight: dict[tuple[str, str], asyncio.Future] = {}
        self._unseen: dict[str, datetime] = {}  # open, not enqueued: first seen by sweep
        self._cancelling: set[asyncio.Task] = set()  # cancels answered 202, still cleaning up
        db.migrate("front_ask_keys_v1", _SCHEMA)

    def routes(self) -> list[Route]:
        return [
            Route("/internal/health", self.health, methods=["GET"]),
            Route("/internal/runs/{run_id}/asks", self.post_ask, methods=["POST"]),
            Route("/internal/runs/{run_id}/evidence", self.post_evidence, methods=["POST"]),
            Route("/internal/runs/{run_id}/cancel", self.post_cancel, methods=["POST"]),
        ]

    def _emit(self, kind: str, run_id: str | None, payload: dict) -> None:
        try:
            self._emitter(kind, payload, run_id=run_id)
        except Exception:  # noqa: BLE001 - audit logging never fails a request
            log.warning("event append failed", exc_info=True)

    # --- health --------------------------------------------------------------------------

    async def health(self, request: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    # --- asks ----------------------------------------------------------------------------

    async def post_ask(self, request: Request) -> JSONResponse:
        run_id = request.path_params["run_id"]
        try:
            draft = _ASK.validate_json(await request.body())
        except ValidationError as e:
            return _error(422, f"invalid AskDraft: {e.error_count()} error(s)")
        key = (request.headers.get(IDEMPOTENCY_HEADER) or "").strip()[:KEY_MAX] or None
        try:
            ticket = await self._raise(run_id, draft, key)
        except Exception as e:  # noqa: BLE001 - the worker sees a failure and may retry
            log.exception("raise_ask failed for %s", run_id)
            self._emit("front.ask_failed", run_id, {"error": repr(e)[:ERROR_MAX]})
            return _error(500, error_text(e, ERROR_MAX))
        return JSONResponse(ticket)

    async def _raise(self, run_id: RunId, draft: AskDraft, key: str | None) -> dict:
        if key is None:
            return await self._raise_new(run_id, draft, None)
        replay = self._replay(run_id, key)
        if replay is not None:
            await self._announce(replay["ask_id"])  # outbox dedupes on ref_id
            return replay
        slot = (run_id, key)
        if slot in self._inflight:  # a retry while the first request is still raising
            return await asyncio.shield(self._inflight[slot])
        fut = asyncio.ensure_future(self._raise_new(run_id, draft, key))
        self._inflight[slot] = fut
        fut.add_done_callback(lambda _: self._inflight.pop(slot, None))
        return await asyncio.shield(fut)

    def _open_view(self, ask_id: str) -> AskView | None:
        return next((a for a in self._inbox.open_asks() if a["ask_id"] == ask_id), None)

    def _replay(self, run_id: RunId, key: str) -> dict | None:
        r = self._db.execute(
            "SELECT ask_id, ticket_json FROM front_ask_keys WHERE run_id=? AND key=?",
            (run_id, key),
        ).fetchone()
        if r is None or self._open_view(r["ask_id"]) is None:
            return None  # unknown key, or that Ask closed: this is a new question
        self._emit("front.ask_replayed", run_id, {"ask_id": r["ask_id"]})
        return json.loads(r["ticket_json"])

    async def _raise_new(self, run_id: RunId, draft: AskDraft, key: str | None) -> dict:
        ticket = await self._pool.raise_ask(run_id, draft)
        out: dict[str, Any] = {
            "ask_id": ticket["ask_id"],
            "outcome": ticket.get("outcome"),
            "number": ticket.get("number") or self._inbox.topic_number(ticket["ask_id"]),
        }
        if key is not None:
            self._db.execute(
                "INSERT OR REPLACE INTO front_ask_keys(run_id, key, ask_id, ticket_json,"
                " created_at) VALUES (?,?,?,?,?)",
                # not to_json: `default=str` writes a datetime as "date time", not "dateTtime"
                (run_id, key, out["ask_id"], json.dumps(out, ensure_ascii=False, default=str),
                 self._clock.now().isoformat()),
            )  # fmt: skip
        if out["outcome"] is None:
            await self._announce(out["ask_id"])
        return out

    def _channel_of(self, view: AskView) -> ChannelRef:
        if view["project_id"]:
            with contextlib.suppress(KeyError, ValueError):  # like Coordinator._channel_of
                return self._projects.get(view["project_id"])["channel"]
        return self._general

    async def _announce(self, ask_id: str, view: AskView | None = None) -> bool:
        """Send the open Ask to its topic (General if that topic can't be used). Urgent
        (ignores quiet hours) only when the work is blocked on a human.
        Returns True once it is in the outbox; failures are healed by `sweep()`."""
        try:
            view = view or self._open_view(ask_id)
            if view is None:
                return False
            n = self._inbox.topic_number(ask_id)
            msg = Outbound(
                channel=self._channel_of(view),
                kind="ask",
                text=render_ask(n, view),
                ref_id=ask_id,
                urgent=is_blocked(view),
            )
            try:
                await self._notifier.send(msg)
            except ValueError:  # the project's channel is not deliverable: use General
                if msg["channel"] == self._general:
                    raise
                await self._notifier.send({**msg, "channel": self._general})
            return True
        except Exception as e:  # noqa: BLE001 - the Ask exists; the sweep re-announces
            log.exception("announcing ask %s failed", ask_id)
            self._emit("front.ask_notify_failed", None, {"ask_id": ask_id, "error": repr(e)})
            return False

    # --- announce sweep ------------------------------------------------------------------

    async def sweep(self, *, grace: timedelta = SWEEP_GRACE) -> int:
        """Announce open Asks the outbox has never seen; prune idempotency keys of closed
        Asks. An Ask is announced only after it was seen unannounced for `grace` (the
        raiser — route or Coordinator — enqueues it within moments). Returns how many."""
        now = self._clock.now()
        views = self._inbox.open_asks()
        open_ids = {v["ask_id"] for v in views}
        for aid in list(self._unseen):
            if aid not in open_ids:
                del self._unseen[aid]
        healed = 0
        for v in views:
            aid = v["ask_id"]
            if self._notifier.has_ask(aid):
                self._unseen.pop(aid, None)
                continue
            first = self._unseen.setdefault(aid, now)
            if now - first < grace:
                continue
            if await self._announce(aid, v):
                self._unseen.pop(aid, None)
                healed += 1
                self._emit("front.ask_swept", v["run_id"], {"ask_id": aid})
        with self._db.transaction():
            for r in self._db.execute("SELECT run_id, key, ask_id FROM front_ask_keys").fetchall():
                if r["ask_id"] not in open_ids:  # replay only ever serves open Asks
                    self._db.execute(
                        "DELETE FROM front_ask_keys WHERE run_id=? AND key=?",
                        (r["run_id"], r["key"]),
                    )
        return healed

    async def run_sweeps(self, stop: asyncio.Event) -> None:
        """Startup sweep with no grace (nothing of ours is in flight yet), then every 60 s."""
        grace = timedelta(0)
        while not stop.is_set():
            try:
                await self.sweep(grace=grace)
            except Exception:  # noqa: BLE001 - the loop outlives a busy DB
                log.exception("announce sweep failed")
            grace = SWEEP_GRACE
            await pause(SWEEP_EVERY_S, stop)

    # --- evidence ------------------------------------------------------------------------

    async def post_evidence(self, request: Request) -> JSONResponse:
        run_id = request.path_params["run_id"]
        try:
            raw = json.loads(await request.body())
            if not isinstance(raw, dict):
                raise ValueError("body must be an object")
            # the {claim, screenshot_path, ts} shape
            if "claim" in raw or "ts" in raw or "screenshot_path" in raw:
                raw = {
                    "url": raw.get("url"),
                    "screenshot": raw.get("screenshot", raw.get("screenshot_path")),
                    "captured_at": raw.get("captured_at", raw.get("ts")),
                    "text": raw.get("text", raw.get("claim")),
                }
            ev = _EVIDENCE.validate_python(raw)
        except (ValueError, ValidationError) as e:
            return _error(422, f"invalid Evidence: {type(e).__name__}")
        at: datetime = ev["captured_at"]
        if at.tzinfo is None:
            ev["captured_at"] = at.replace(tzinfo=zone_of(self._clock))
        evidence_id = new_id("ev")
        ev["id"] = evidence_id  # stored with the item (pool keeps it in evidence_json)
        try:
            self._pool.add_evidence(run_id, ev)
        except KeyError:
            return _error(404, f"unknown run {run_id}")
        except Exception as e:  # noqa: BLE001 - e.g. a finished run
            log.warning("add_evidence failed for %s", run_id, exc_info=True)
            return _error(409, error_text(e, ERROR_MAX))
        return JSONResponse({"evidence_id": evidence_id})

    # --- cancel --------------------------------------------------------------------------

    async def post_cancel(self, request: Request) -> JSONResponse:
        run_id = request.path_params["run_id"]
        try:
            raw = json.loads(await request.body())
            reason = raw.get("reason") if isinstance(raw, dict) else None
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("reason must be a non-empty string")
        except ValueError as e:
            return _error(422, f"invalid cancel body: {e}"[:ERROR_MAX])
        reason = reason.strip()[:REASON_MAX]
        self._emit("front.run_cancel_requested", run_id, {"reason": reason})
        task = asyncio.ensure_future(self._pool.cancel(run_id, reason=reason))
        self._cancelling.add(task)
        task.add_done_callback(self._cancel_done)
        try:
            await asyncio.wait_for(asyncio.shield(task), CANCEL_WAIT_S)
        except TimeoutError:
            return JSONResponse({"ok": True, "run_id": run_id, "pending": True}, status_code=202)
        except KeyError:
            return _error(404, f"unknown run {run_id}")
        except Exception as e:  # noqa: BLE001
            log.warning("cancel failed for %s", run_id, exc_info=True)
            return _error(409, error_text(e, ERROR_MAX))
        return JSONResponse({"ok": True, "run_id": run_id})

    def _cancel_done(self, task: asyncio.Task) -> None:
        self._cancelling.discard(task)
        if not task.cancelled() and task.exception() is not None:
            task.exception()  # retrieved: the route (or the log above) reported it

    async def drain(self) -> None:
        """Shutdown: wait for cancels still cleaning up in the background."""
        if self._cancelling:
            await asyncio.gather(*list(self._cancelling), return_exceptions=True)


# --- app + guard ---------------------------------------------------------------------------


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class BearerGuard:
    """Pure ASGI: loopback clients only; bearer secret on every path but OPEN_PATHS."""

    def __init__(self, app: ASGIApp, secret: str) -> None:
        self.app = app
        self._secret = f"Bearer {secret}".encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)  # lifespan
            return
        client = scope.get("client")
        if not _is_loopback(client[0] if client else None):
            await self._deny(scope, receive, send, 403, "loopback only", auth=False)
            return
        if scope["path"] not in OPEN_PATHS:
            got = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not hmac.compare_digest(got, self._secret):
                await self._deny(scope, receive, send, 401, "bearer token required", auth=True)
                return
        await self.app(scope, receive, send)

    @staticmethod
    async def _deny(scope, receive, send, status: int, message: str, *, auth: bool) -> None:
        if auth:  # method, path, peer only: never the header, it may hold a near-miss secret
            client = scope.get("client")
            log.warning(
                "internal API: bearer rejected (401) %s %s from %s",
                scope.get("method", "WS"), scope["path"], client[0] if client else "?",
            )  # fmt: skip
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        headers = {"WWW-Authenticate": 'Bearer realm="swarmd"'} if auth else None
        resp = JSONResponse({"error": message}, status_code=status, headers=headers)
        await resp(scope, receive, send)


def build_app(*, secret: str, api: InternalApi, mcp_app: Starlette | None = None) -> Starlette:
    """One Starlette app behind the guard: the internal routes, plus SwarmFront MCP
    (streamable HTTP at /mcp) when `mcp_app` (front.mcp_server.streamable_app) is given.
    The MCP app is the base so its lifespan (session manager) runs under uvicorn."""
    if not secret:
        raise ValueError("internal API secret is empty")
    app = mcp_app if mcp_app is not None else Starlette()
    app.router.routes[:0] = api.routes()
    app.add_middleware(BearerGuard, secret=secret)
    return app


class _Server(uvicorn.Server):
    """uvicorn that leaves signals alone: swarmd owns SIGTERM/SIGINT (ordered shutdown).
    Stock `serve()` replaces the handlers and re-raises the signal after it stops."""

    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


def make_server(app: ASGIApp, port: int, *, graceful_s: float = 5.0) -> uvicorn.Server:
    """uvicorn on 127.0.0.1 only (the bind is not configurable on purpose)."""
    return _Server(
        uvicorn.Config(
            app, host=LOOPBACK, port=port, log_level="warning", access_log=False,
            timeout_graceful_shutdown=graceful_s,
        )
    )  # fmt: skip
