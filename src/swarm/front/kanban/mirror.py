"""HermesKanbanMirror: the human's kanban is the Hermes dashboard's built-in
board, used as a mirror. ProjectStore (backlog) and the run table are canonical.

Outbound (↓ CardView):
- `upsert(card)` stores the card as the desired state in SQLite, marks it dirty and returns;
  it never raises for Hermes trouble. `flush()` pushes dirty cards in order: a create for a
  new card (idempotent by card key), an update after that. We never send an `assignee`, so
  Hermes's dispatcher never runs our cards as Hermes work.
- the dirty flags live in SQLite, so a dashboard that is down keeps the writes across a
  restart. Transient failures back off 30 s, 1 m, 2 m ... capped at 10 m (`mirror.retry`); a
  card Hermes rejects (4xx) is dead-lettered (`mirror.dead`) without blocking the others.
  401/403 back off the same way but are reported once per episode (`mirror.auth_failed` + one
  FYI). After a recovery the run loop resyncs every board. A card that changes board has its
  old task archived.
- body = intent + done criteria + latest summary (item note / latest run result) + artifact
  paths and evidence links as lines (no upload yet). Runs of a backlog item update that
  item's card; runs without one get their own card.
- done cards are shown as `archived` once they have been done for 14 days.

Inbound (↑ KanbanChange, WS `.../events?since=<id>`, the id persisted after every event):
- echoes of our own writes are dropped by the `(task_id, status+priority, updated_at)` we
  recorded from each write's reply (updated_at compared as an instant). A remote field is a
  human change only if it differs from both the canonical card and what we last pushed, the
  state is not older than our last write, and no write of ours is queued or in flight.
- the canon rule: moving a backlog card up/down (priority) -> "reordered", dropping it to
  `archived` -> "archived", a comment -> "commented"; those are proposals the Coordinator
  applies (`change_event` -> `human.kanban_reorder` / `_archive` / `_comment`), and their
  result comes back through the feed. Every other status move ("moved", e.g. running -> done)
  is put back right away by re-pushing the canonical card (`mirror.reverted`), at most 3
  times per card in 10 minutes: then `mirror.contested` + one FYI and the next resync retries.
  A proposal the Coordinator has not taken after 5 minutes is reverted by the next resync.
- human comments count once (ids persisted) and only from creation events.
- `resync(project_id)` (every 10 minutes and after a reconnect / recovery) rebuilds the
  project's cards from the store, reads the board, reports allowed human changes made while
  we were away (inbound first), reverts the rest, recreates deleted tasks and flushes. The
  board read and the comparison run under the flush lock.
- `run()` supervises its four loops: one that dies is logged (`mirror.loop_died`) and
  restarted with backoff. Our own events are followed by log seq (`EventLog.tail`).

Every Hermes URL, header and payload shape is in `hermes.HermesTransport` (Hermes's API is not
yet pinned down); every statement on the `kanban_*` tables is in `store.MirrorTables`. The optional
session token is redacted from every error we log or store.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import sqlite3
from collections import deque
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx

from swarm.aio import pause
from swarm.backoff import doubling
from swarm.clock import Clock
from swarm.front.kanban.cards import (
    ARCHIVE_AFTER,
    BODY_MAX,
    CARD_STATUSES,
    CARD_TITLE_MAX,
    FEED_KINDS,
    card_fields,
    card_from_backlog,
    card_from_run,
    change_event,
    result_extras,
)
from swarm.front.kanban.hermes import (
    TOKEN_HEADER,
    HermesAuthFailed,
    HermesNotFound,
    HermesRejected,
    HermesTransport,
    HermesUnavailable,
    RemoteComment,
    RemoteTask,
    WsConn,
    WsConnect,
    event_id,
    parse_event,
)
from swarm.front.kanban.store import MirrorTables
from swarm.projects.store import ProjectStore
from swarm.redact import redact
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.text import clip
from swarm.types import (
    BacklogItem,
    CardView,
    Event,
    KanbanChange,
    Project,
    ProjectId,
    RunId,
    RunResult,
    WorkerRun,
    WorkOrder,
)

log = logging.getLogger(__name__)

RESYNC_EVERY_S = 600
BACKOFF_BASE_S = 30
BACKOFF_CAP_S = 600
PROPOSAL_GRACE = timedelta(minutes=5)
REVERT_BUDGET = 3  # reverts of one card within REVERT_WINDOW before the mirror backs off
REVERT_WINDOW = timedelta(minutes=10)
ECHO_KEEP = timedelta(days=1)
FEED_POLL_S = 1.0
FLUSH_POLL_S = 60.0
DIGEST_CHARS = 16  # hex chars of a card body hash used as its key
LOOP_ERROR_MAX = 500

_ORDER_ID = re.compile(r"^wo_[0-9A-Z]{26}$")  # the note a running item carries: not a summary


class KanbanMirror(Protocol):
    """Frozen signature: the mirror the coordinator and front depend on."""

    def upsert(self, card: CardView) -> None: ...  # queued; failures retried, never raised
    def inbound(self) -> Iterator[KanbanChange]: ...  # human edits only, no echoes
    async def resync(self, project_id: ProjectId) -> None: ...  # board := canonical


class RunReader(Protocol):
    """What the mirror reads about runs: the read-only slice of `WorkerPool`. Its siblings
    are `front.internal_api.WorkerEndpoints` and the Coordinator's `RunControl`."""

    def get(self, run_id: RunId) -> WorkerRun: ...
    def running(self) -> list[WorkerRun]: ...
    def order(self, run_id: RunId) -> WorkOrder | None: ...
    def result(self, run_id: RunId) -> RunResult | None: ...


def _instant(ts: str | None) -> str:
    """updated_at as a comparable UTC instant ("" when absent); raw text if unparseable."""
    if not ts:
        return ""
    try:
        d = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return ts
    return d.astimezone(UTC).isoformat() if d.tzinfo else ts


def _state(status: str | None, priority: int | None) -> str:
    """The part of a task the mirror interprets; the echo key hashes it (text fields are
    left out: Hermes may normalize them, and inbound never reads them)."""
    return json.dumps({"status": status, "priority": priority}, sort_keys=True)


def _older(a: str, b: str) -> bool:
    """a strictly before b, both tz-aware instants; False when either is unknown."""
    try:
        da, db = datetime.fromisoformat(a), datetime.fromisoformat(b)
    except ValueError:
        return False
    return da.tzinfo is not None and db.tzinfo is not None and da < db


# --- the mirror ---------------------------------------------------------------------------


class HermesKanbanMirror:
    """The KanbanMirror on the Hermes dashboard: feeds our events into cards, flushes them,
    listens for the human's changes and resyncs every board."""

    def __init__(
        self,
        base_url: str,
        db: Database,
        log: EventLog,
        clock: Clock,
        *,
        projects: ProjectStore,
        runs: RunReader | None = None,
        token: str | None = None,
        token_header: str = TOKEN_HEADER,
        transport: httpx.AsyncBaseTransport | None = None,
        ws_connect: WsConnect | None = None,
        ws_retry_s: tuple[float, float] = (1.0, 60.0),
        loop_retry_s: tuple[float, float] = (1.0, 60.0),
        alert: Callable[[ProjectId | None, str], Awaitable[None]] | None = None,
        timeout_s: float = 10.0,
    ) -> None:
        self._t = HermesTransport(
            base_url, token=token, token_header=token_header, transport=transport,
            ws_connect=ws_connect, timeout_s=timeout_s,
        )  # fmt: skip
        self._db = db
        self._log = log
        self._clock = clock
        self._emit = Emitter(log, clock, "kanban", best_effort=True)
        self._projects = projects
        self._runs = runs
        self._ws_retry = ws_retry_s
        self._loop_retry = loop_retry_s
        self._inbound: deque[KanbanChange] = deque()
        self._lock = asyncio.Lock()  # one flush at a time
        self._wake = asyncio.Event()  # something became dirty
        self._resync_wanted = asyncio.Event()  # reconnect / recovery: reconcile every board
        self._inflight: set[str] = set()  # card keys whose write is on the wire
        self._alert_cb = alert  # one FYI to the human (contested card, auth failure)
        self._bg: set[asyncio.Task] = set()
        self._tables = MirrorTables(db)
        self._kv = self._tables.kv

    def __repr__(self) -> str:
        return f"HermesKanbanMirror({self._t!r})"

    # --- plumbing ------------------------------------------------------------------------

    def _now(self) -> datetime:
        return self._clock.now()

    def _mark_dirty(self, key: str) -> None:
        self._tables.mark_dirty(key)
        self._wake.set()

    def _project_of_board(self, board: str) -> ProjectId | None:
        with contextlib.suppress(Exception):
            return next((p["id"] for p in self._projects.list() if p["board"] == board), None)
        return None

    def _project_of_key(self, key: str) -> ProjectId | None:
        r = self._tables.card(key)
        return self._project_of_board(r["board"]) if r else None

    def _is_backlog(self, key: str) -> bool:
        return any(b["id"] == key for p in self._projects.list() for b in p["backlog"])

    # --- card content -------------------------------------------------------------------

    def _item_card(self, item: BacklogItem, project: Project) -> CardView:
        return card_from_backlog(item, project, **self._tables.extras(item["id"]))

    def _run_card(self, run: WorkerRun, order: WorkOrder, project: Project) -> CardView:
        return card_from_run(run, order, project, **self._tables.extras(run["id"]))

    # --- outbound ------------------------------------------------------------------------

    def upsert(self, card: CardView) -> None:
        """Store `card` as the canonical state of its key and queue it. Never talks to Hermes."""
        status = "archived" if card["status"] == "dropped" else card["status"]
        if status not in CARD_STATUSES:
            raise ValueError(f"unknown card status {card['status']!r}")
        now = self._now()
        with self._db.transaction():
            row = self._tables.card(card["key"])
            done_at = row["done_at"] if row else None
            if status == "done":
                done_at = done_at or now.isoformat()
                if now - datetime.fromisoformat(done_at) >= ARCHIVE_AFTER:
                    status = "archived"  # old done work leaves the board
            elif status != "archived":
                done_at = None
            stored = {
                "key": card["key"], "board": card["board"],
                "title": clip(card["title"], CARD_TITLE_MAX), "body": clip(card["body"], BODY_MAX),
                "status": status, "priority": int(card["priority"]),
                "links": [str(u) for u in card["links"]],
                "attachments": [str(p) for p in card["attachments"]],
            }  # fmt: skip
            raw = json.dumps(stored, ensure_ascii=False, sort_keys=True)
            if row is not None and row["card_json"] == raw and not row["dead"]:
                return  # nothing new to show
            if row is None:
                self._tables.add_card(card["key"], card["board"], raw, status,
                                      stored["priority"], done_at, now.isoformat())  # fmt: skip
            else:
                moved_board = row["board"] != card["board"]
                if moved_board and row["task_id"]:  # archive what is left on the old board
                    self._tables.add_orphan(row["task_id"], row["board"], now.isoformat())
                self._tables.put_card(
                    card["key"], card["board"], raw, status, stored["priority"], done_at,
                    now.isoformat(), None if moved_board else row["task_id"],
                )  # fmt: skip
                self._mark_dirty(card["key"])
        self._wake.set()

    def comment(self, key: str, text: str, *, ref: str) -> None:
        """Queue a comment of ours on `key`'s card, at most once per `ref`."""
        if self._tables.add_comment(ref, key, clip(text, BODY_MAX), self._now().isoformat()):
            self._wake.set()

    async def flush(self, *, force: bool = False, reached: bool = False) -> int:
        """Push queued cards, then queued comments. Outside the backoff window unless forced.
        Returns how many cards were written. `reached`: the caller just got a reply from
        Hermes (resync's board read), so an empty queue still proves a recovery."""
        async with self._lock:
            due = self._kv.get("next_attempt_at")
            if not force and due is not None and datetime.fromisoformat(due) > self._now():
                return 0
            return await self._flush_locked(reached=reached)

    async def _flush_locked(self, *, reached: bool) -> int:
        pushed = sent = 0
        try:
            for o in self._tables.orphans():
                sent += await self._archive_orphan(o)
            for r in self._tables.dirty_cards():
                pushed += await self._push(r)
            for c in self._tables.pending_comments():
                sent += await self._push_comment(c)
        except HermesUnavailable as e:
            self._backoff(e)
            return pushed
        if self._kv.get("attempts") is not None and (pushed or sent or reached):
            self._kv.set("attempts", None)
            self._kv.set("next_attempt_at", None)
            self._kv.set("auth_alerted", None)
            self._emit("mirror.recovered", {"pushed": pushed})
            self._resync_wanted.set()
        self._tables.prune_echoes((self._now() - ECHO_KEEP).isoformat())
        return pushed

    async def _push(self, r) -> int:
        card = json.loads(r["card_json"])
        fields = card_fields(card)
        task: RemoteTask | None = None
        self._inflight.add(r["key"])
        try:
            if r["task_id"]:
                try:
                    task = await self._t.update_task(card["board"], r["task_id"], fields)
                except HermesNotFound:  # deleted on the board: the canonical card comes back
                    task = None
            if task is None:
                task = await self._t.create_task(card["board"], card["key"], fields)
        except HermesRejected as e:
            self._tables.mark_dead(r["key"], r["dirty_seq"], str(e))
            log.error("kanban card %s rejected by Hermes: %s", r["key"], e)
            self._emit(
                "mirror.dead",
                {"key": r["key"], "board": card["board"], "status": e.status, "error": str(e)},
                project_id=self._project_of_board(card["board"]),
            )
            return 0
        finally:
            self._inflight.discard(r["key"])
        status = task.status or fields["status"]
        priority = task.priority if task.priority is not None else fields["priority"]
        at = _instant(task.updated_at)
        pushed = {"status": status, "priority": priority, "updated_at": at}
        with self._db.transaction():
            self._tables.set_pushed(r["key"], r["dirty_seq"], task.id, pushed)
            self._tables.record_echo(task.id, _state(status, priority), at, self._now().isoformat())
        return 1

    async def _push_comment(self, c) -> int:
        row = self._tables.card(c["key"])
        if row is None or not row["task_id"]:
            return 0  # the card is not on the board yet
        board = json.loads(row["card_json"])["board"]
        try:
            remote_id = await self._t.add_comment(board, row["task_id"], c["text"])
        except HermesRejected as e:
            self._tables.comment_dead(c["seq"], str(e))
            log.error("kanban comment %s rejected by Hermes: %s", c["ref"], e)
            return 0  # a dead letter proves nothing about recovery
        self._tables.comment_sent(c["seq"], remote_id or None)
        return 1

    async def _archive_orphan(self, o) -> int:
        """A card that moved to another board: archive its task on the old board."""
        try:
            await self._t.update_task(o["board"], o["task_id"], {"status": "archived"})
        except HermesRejected as e:  # gone already (404) or refused: nothing more to do
            log.warning("kanban orphan %s on %s not archived: %s", o["task_id"], o["board"], e)
            self._tables.drop_orphan(o["task_id"])
            return 0
        self._tables.drop_orphan(o["task_id"])
        return 1

    def _backoff(self, e: HermesUnavailable) -> None:
        error = str(e)
        attempts = int(self._kv.get("attempts") or 0) + 1
        wait = doubling(attempts, BACKOFF_BASE_S, BACKOFF_CAP_S)
        self._kv.set("attempts", str(attempts))
        self._kv.set("next_attempt_at", (self._now() + timedelta(seconds=wait)).isoformat())
        pending = self._tables.dirty_count()
        if isinstance(e, HermesAuthFailed):  # one alert per episode, not a retry every 10 min
            if self._kv.get("auth_alerted") is None:
                self._kv.set("auth_alerted", self._now().isoformat())
                log.error("hermes kanban refused our credentials (%s); %d cards queued",
                          error, pending)  # fmt: skip
                self._emit("mirror.auth_failed", {"error": error, "pending": pending})
                self._send_alert(
                    None,
                    "⚠️ FYI · Hermes 칸반이 인증을 거절해요 "
                    f"({error}). config.toml의 hermes_session_token을 확인해 주세요. "
                    "칸반 반영은 큐에 쌓아 두고 복구되면 맞춥니다.",
                )
            else:
                log.debug("hermes kanban still refusing credentials (try %d)", attempts)
            return
        log.warning("hermes kanban unavailable (try %d, %d queued), retry in %ss: %s",
                    attempts, pending, wait, error)  # fmt: skip
        self._emit(
            "mirror.retry",
            {"attempt": attempts, "wait_s": wait, "pending": pending, "error": error},
        )

    # --- inbound -------------------------------------------------------------------------

    def inbound(self) -> Iterator[KanbanChange]:
        """Human changes received so far (each yielded once)."""
        while self._inbound:
            yield self._inbound.popleft()

    def _ingest(self, msg: dict) -> str | None:
        eid, obj = parse_event(msg)
        if isinstance(obj, RemoteComment):
            self._ingest_comment(obj)
        elif isinstance(obj, RemoteTask):
            row = self._tables.card_by_task(obj.id)
            if row is None:
                return eid  # not one of our cards
            at = _instant(obj.updated_at)
            if self._tables.take_echo(obj.id, _state(obj.status, obj.priority), at):
                return eid  # our own write coming back
            if row["dirty"] or row["key"] in self._inflight:
                return eid  # a write of ours is queued / on the wire: it decides the board
            self._consider(row, obj, resync=False)
        return eid

    def _ingest_comment(self, c: RemoteComment) -> None:
        key = self._tables.key_of_task(c.task_id)
        if key is None or not c.body.strip():
            return
        ckey = c.id or "sha:" + hashlib.sha256(c.body.encode()).hexdigest()[:DIGEST_CHARS]
        if not self._tables.mark_comment_seen(c.task_id, ckey, self._now().isoformat()):
            return  # replayed / re-delivered: handled already
        if not self._tables.is_our_comment(c.id, key, c.body):
            self._inbound.append(
                KanbanChange(card_key=key, kind="commented", to_status=None,
                             priority=None, comment=clip(c.body, BODY_MAX))
            )  # fmt: skip

    def _consider(self, row, t: RemoteTask, *, resync: bool) -> None:
        """Canon check of a remote state. A field is a human change only if it differs from
        both the canonical card and what we last pushed, and the state is not older than our
        last write (a late echo / snapshot of an earlier write of ours)."""
        key = row["key"]
        pushed = json.loads(row["pushed_json"]) if row["pushed_json"] else {}
        if _older(_instant(t.updated_at), pushed.get("updated_at", "")):
            return
        status = t.status if t.status in CARD_STATUSES else None
        human_status = None if status in (row["status"], pushed.get("status")) else status
        human_priority = (
            None if t.priority in (row["priority"], pushed.get("priority")) else t.priority
        )
        seen = {"status": t.status, "priority": t.priority}
        if human_status is None and human_priority is None:
            if resync and row["contested"]:  # board and canon agree again
                self._tables.set_contested(key, False)
            return
        if human_status is not None:
            if human_status == "archived" and self._is_backlog(key):
                self._propose(row, seen, resync, KanbanChange(
                    card_key=key, kind="archived", to_status="archived", priority=None,
                    comment=None))  # fmt: skip
                return
            if self._revert(row, t, force=resync):
                self._inbound.append(
                    KanbanChange(card_key=key, kind="moved", to_status=human_status,  # type: ignore[typeddict-item]
                                 priority=None, comment=None)
                )  # fmt: skip
        else:
            if self._is_backlog(key):
                self._propose(row, seen, resync, KanbanChange(
                    card_key=key, kind="reordered", to_status=None, priority=human_priority,
                    comment=None))  # fmt: skip
            else:
                self._revert(row, t, force=resync)

    def _propose(self, row, seen: dict, resync: bool, change: KanbanChange) -> None:
        raw = json.dumps(seen, sort_keys=True)
        if row["proposal_json"] == raw:  # proposed already; the Coordinator has not taken it
            at = datetime.fromisoformat(row["proposal_at"])
            if resync and self._now() - at >= PROPOSAL_GRACE:
                self._revert(
                    row,
                    RemoteTask(row["task_id"], seen["status"], seen["priority"], None),
                    force=True,
                )
            return
        self._tables.set_proposal(row["key"], raw, self._now().isoformat())
        self._inbound.append(change)

    def _revert(self, row, t: RemoteTask, *, force: bool = False) -> bool:
        """Put the canonical card back. At most REVERT_BUDGET times per card within
        REVERT_WINDOW (a second writer would otherwise fight us forever): then the card is
        left alone, `mirror.contested` + one FYI, and the next resync (force) retries."""
        now = self._now()
        key = row["key"]
        self._tables.prune_reverts((now - REVERT_WINDOW).isoformat())
        recent = self._tables.revert_count(key)
        if not force and recent >= REVERT_BUDGET:
            cur = self._tables.card(key)
            if cur is not None and not cur["contested"]:
                self._tables.set_contested(key, True)
                pid = self._project_of_board(row["board"])
                title = json.loads(row["card_json"])["title"]
                log.warning("kanban card %s contested: %d reverts in %s, backing off", key,
                            recent, REVERT_WINDOW)  # fmt: skip
                self._emit(
                    "mirror.contested",
                    {"key": key, "task_id": t.id, "board": row["board"], "reverts": recent,
                     "human_status": t.status, "status": row["status"]},
                    project_id=pid,
                )  # fmt: skip
                self._send_alert(
                    pid,
                    f"ℹ️ FYI · 칸반 카드 '{title}'를 누군가 계속 옮겨요. "
                    f"{int(REVERT_WINDOW.total_seconds() // 60)}분 안에 {recent}번 되돌린 뒤 "
                    f"멈췄어요 (정본: {row['status']}). 다음 동기화 때 다시 맞춥니다.",
                )
            return False
        self._tables.record_revert(key, now.isoformat())
        self._mark_dirty(row["key"])
        log.info("kanban card %s: human set %s/%s, reverting to %s/%s", row["key"], t.status,
                 t.priority, row["status"], row["priority"])  # fmt: skip
        self._emit(
            "mirror.reverted",
            {"key": row["key"], "task_id": t.id, "board": row["board"],
             "human_status": t.status, "human_priority": t.priority,
             "status": row["status"], "priority": row["priority"]},
            project_id=self._project_of_board(row["board"]),
        )  # fmt: skip
        return True

    def _send_alert(self, project_id: ProjectId | None, text: str) -> None:
        """Fire-and-forget FYI through the wired callback (none wired: the event suffices)."""
        if self._alert_cb is None:
            return
        cb = self._alert_cb

        async def send() -> None:
            try:
                await cb(project_id, text)
            except Exception as e:  # noqa: BLE001 - an FYI never breaks mirroring
                log.warning("kanban alert not sent: %s", redact(repr(e)))

        with contextlib.suppress(RuntimeError):  # no running loop: nothing to send from
            task = asyncio.get_running_loop().create_task(send())
            self._bg.add(task)
            task.add_done_callback(self._bg.discard)

    # --- resync ---------------------------------------------------------------------------

    async def resync(self, project_id: ProjectId) -> None:
        """Board := canonical. Human changes made while we were away go to inbound first."""
        project = self._projects.get(project_id)
        board = project["board"]
        for b in project["backlog"]:
            self.upsert(self._item_card(b, project))
        if self._runs is not None:
            for run in self._runs.running():
                order = self._runs.order(run["id"])
                if order and order["project_id"] == project_id and not order["backlog_item_id"]:
                    self.upsert(self._run_card(run, order, project))
        self._archive_stale(board)
        async with self._lock:  # no flush between the board read and the comparison
            await self._reconcile(project_id, board)

    async def _reconcile(self, project_id: ProjectId, board: str) -> None:
        try:
            remote = {t.id: t for t in await self._t.list_tasks(board)}
        except HermesUnavailable as e:
            self._backoff(e)
            return
        except HermesRejected as e:
            log.error("kanban resync of %s failed: %s", board, e)
            self._emit(
                "mirror.resync_failed", {"board": board, "error": str(e)}, project_id=project_id
            )
            return
        rows = self._tables.board_cards(board)
        recreated = 0
        for row in rows:
            t = remote.get(row["task_id"])
            if t is None:
                self._tables.forget_task(row["key"])
                self._mark_dirty(row["key"])
                recreated += 1
            elif not row["dirty"]:  # a write of ours still queued wins over the board
                self._consider(row, t, resync=True)
        pushed = await self._flush_locked(reached=True)
        self._emit(
            "mirror.resync",
            {"board": board, "cards": len(rows), "recreated": recreated, "pushed": pushed},
            project_id=project_id,
        )

    def _archive_stale(self, board: str) -> None:
        for card in self._tables.done_cards(board):
            self.upsert(card)  # upsert turns 14-day-old done into archived

    # --- feed: our events -> upserts --------------------------------------------------------

    async def feed_once(self) -> None:
        """Mirror backlog / run / ask events logged after the persisted log position (seq,
        so an event stamped earlier than one already read is never skipped)."""
        after = int(self._kv.get("feed_seq") or 0)
        while True:
            batch = self._log.tail(after_seq=after, kinds=FEED_KINDS)
            for seq, ev in batch:
                try:
                    self._feed(ev)
                except Exception as e:  # noqa: BLE001 - one bad event never stops the feed
                    log.warning("kanban feed skipped %s %s: %r", ev["kind"], ev["id"], e)
                after = seq
                self._kv.set("feed_seq", str(seq))
            if not batch:
                return

    def _feed(self, ev: Event) -> None:
        kind, p = ev["kind"], ev["payload"]
        if kind.startswith("project.backlog_"):
            if not ev["project_id"]:
                return
            project = self._projects.get(ev["project_id"])
            item = next((b for b in project["backlog"] if b["id"] == p.get("item_id")), None)
            if item is None:
                return
            note = str(p.get("note") or "").strip()
            if kind == "project.backlog_updated" and note and not _ORDER_ID.match(note):
                self._tables.set_extras(item["id"], summary=note)
            self.upsert(self._item_card(item, project))
            return
        if self._runs is None or not ev["run_id"]:
            return
        order = self._runs.order(ev["run_id"])
        if order is None or not order["project_id"]:
            return
        if kind == "ask.raised":  # a blocked card gets the Ask's number as a comment
            key = order["backlog_item_id"] or ev["run_id"]
            text = (f"질문 #{p.get('number')} 대기 중 ({p.get('kind')}) — "
                    "Telegram 프로젝트 토픽에서 답해 주세요.")  # fmt: skip
            self.comment(key, text, ref=f"ask:{p.get('ask_id')}")
            return
        project = self._projects.get(order["project_id"])
        item_id = order["backlog_item_id"]
        if kind == "run.finished":  # latest summary + artifacts go on the card
            res = self._runs.result(ev["run_id"])
            if res is not None:
                self._tables.set_extras(item_id or ev["run_id"], **result_extras(res))
            if item_id:
                item = next((b for b in project["backlog"] if b["id"] == item_id), None)
                if item is not None:
                    self.upsert(self._item_card(item, project))
                return
        if not item_id:  # a backlog item's run shows on the item's card
            self.upsert(self._run_card(self._runs.get(ev["run_id"]), order, project))

    # --- WS -------------------------------------------------------------------------------

    async def listen(
        self,
        stop: asyncio.Event,
        on_change: Callable[[KanbanChange], Awaitable[None]] | None = None,
    ) -> None:
        """Follow the board's events until `stop`; reconnect with backoff, resuming after the
        last event id we processed (persisted)."""
        lo, hi = self._ws_retry
        delay = lo
        connected_before = False
        while not stop.is_set():
            try:
                since = self._kv.get("ws_since")
                conn = await self._t.connect_events(since)
            except (HermesUnavailable, sqlite3.Error) as e:
                log.warning("hermes kanban events: connect failed, retry in %.1fs: %s", delay, e)
                if await pause(delay, stop):
                    return
                delay = min(delay * 2, hi)
                continue
            delay = lo
            if connected_before:
                self._resync_wanted.set()  # events from the gap may be gone
            connected_before = True
            try:
                while True:
                    raw = await self._recv(conn, stop)
                    if raw is None:
                        return
                    await self._handle(raw, on_change)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - a dropped socket only means reconnect
                log.warning("hermes kanban events: connection lost: %s", redact(repr(e)))
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
            if await pause(delay, stop):
                return

    @staticmethod
    async def _recv(conn: WsConn, stop: asyncio.Event) -> str | bytes | None:
        recv = asyncio.ensure_future(conn.recv())
        stopper = asyncio.ensure_future(stop.wait())
        try:
            done, _ = await asyncio.wait({recv, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopper.cancel()
        if recv in done:
            return recv.result()
        recv.cancel()
        with contextlib.suppress(BaseException):
            await recv
        return None

    async def _handle(
        self, raw: str | bytes, on_change: Callable[[KanbanChange], Awaitable[None]] | None
    ) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            log.warning("hermes kanban events: not JSON, skipped")
            return
        if not isinstance(msg, dict):
            return
        eid = event_id(msg)
        try:
            eid = self._ingest(msg)
        except Exception as e:  # noqa: BLE001 - resync repairs what one bad event skipped
            log.warning("hermes kanban event %s skipped: %s", eid, redact(repr(e)))
        if on_change is not None:
            for ch in self.inbound():
                try:
                    await on_change(ch)
                except Exception as e:  # noqa: BLE001 - the next change still goes out
                    log.warning("kanban change %s not delivered: %r", ch["card_key"], e)
        if eid is not None:
            self._kv.set("ws_since", eid)

    # --- background loop (wired by the daemon) ---------------------------------------------

    async def run(self, stop: asyncio.Event, *, submit: Callable[[Event], Awaitable[None]]) -> None:
        """Feed (our events -> upserts), flush (with backoff), WS (human changes -> canon ->
        `submit`), resync (at start, every 10 min, after a reconnect / recovery)."""
        if self._kv.get("feed_seq") is None:  # first start: the initial resync covers the past
            self._kv.set("feed_seq", str(self._log.last_seq()))

        async def on_change(ch: KanbanChange) -> None:
            pid = self._project_of_key(ch["card_key"])
            self._emit("mirror.inbound", dict(ch), project_id=pid)
            ev = change_event(ch, project_id=pid, at=self._now())
            if ev is not None:
                await submit(ev)

        loops: dict[str, Callable[[], Awaitable[None]]] = {
            "resync": lambda: self._resync_loop(stop, on_change),
            "ws": lambda: self.listen(stop, on_change),
            "flush": lambda: self._flush_loop(stop),
            "feed": lambda: self._feed_loop(stop),
        }
        tasks = [
            asyncio.create_task(self._supervise(name, start, stop), name=f"kanban-{name}")
            for name, start in loops.items()
        ]
        try:
            await stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _supervise(
        self, name: str, start: Callable[[], Awaitable[None]], stop: asyncio.Event
    ) -> None:
        """Keep one loop alive: a loop that raises (or returns before `stop`) is logged as
        `mirror.loop_died` and restarted with backoff."""
        lo, hi = self._loop_retry
        restarts = 0
        while not stop.is_set():
            try:
                await start()
                if stop.is_set():
                    return
                error = "loop returned"
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - the mirror outlives any one bug
                error = redact(repr(e))
            restarts += 1
            delay = doubling(restarts, lo, hi)
            log.error("kanban %s loop died, restarting in %.1fs: %s", name, delay, error)
            self._emit("mirror.loop_died", {"loop": name, "error": error[:LOOP_ERROR_MAX]})
            if await pause(delay, stop):
                return

    async def _resync_loop(
        self, stop: asyncio.Event, on_change: Callable[[KanbanChange], Awaitable[None]]
    ) -> None:
        while not stop.is_set():
            self._resync_wanted.clear()
            for p in self._safe_list():
                try:
                    await self.resync(p["id"])
                except Exception as e:  # noqa: BLE001 - next project / next round
                    log.error("kanban resync %s failed: %s", p["id"], redact(repr(e)))
                for ch in self.inbound():
                    try:
                        await on_change(ch)
                    except Exception as e:  # noqa: BLE001
                        log.warning("kanban change %s not delivered: %r", ch["card_key"], e)
            if await pause(RESYNC_EVERY_S, stop, self._resync_wanted):
                return

    def _safe_list(self) -> list[Project]:
        try:
            return self._projects.list()
        except Exception as e:  # noqa: BLE001
            log.error("kanban: listing projects failed: %r", e)
            return []

    async def _flush_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self._wake.clear()
            wait = FLUSH_POLL_S
            try:
                await self.flush()
                due = self._kv.get("next_attempt_at")
                if due is not None:
                    left = (datetime.fromisoformat(due) - self._now()).total_seconds()
                    wait = min(max(left, 1.0), FLUSH_POLL_S)
            except Exception as e:  # noqa: BLE001 - the loop outlives any one failure
                log.error("kanban flush failed: %s", redact(repr(e)))
            if await pause(wait, stop, self._wake):
                return

    async def _feed_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.feed_once()
            except Exception as e:  # noqa: BLE001
                log.error("kanban feed failed: %r", e)
            if await pause(FEED_POLL_S, stop):
                return

    async def aclose(self) -> None:
        await self._t.aclose()
