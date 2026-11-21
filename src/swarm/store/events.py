"""Append-only event log over SQLite. No update/delete API by design."""

import contextlib
import json
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, tzinfo
from pathlib import Path
from typing import Any, Protocol

from swarm.clock import SEOUL, Clock
from swarm.ids import new_id
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.types import BlobRef, Event, EventId, ProjectId, RunId

MAX_PAYLOAD_BYTES = 16_384
PREVIEW_CHARS = 1_000

_SCHEMA = """
CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    project_id TEXT,
    run_id TEXT,
    payload_json TEXT NOT NULL,
    refs_json TEXT NOT NULL
);
CREATE INDEX events_run ON events(run_id);
CREATE INDEX events_project ON events(project_id);
CREATE INDEX events_kind_ts ON events(kind, ts)
"""


class EventLog(Protocol):
    def append(self, ev: Event) -> EventId: ...

    def query(
        self,
        *,
        run_id: RunId | None = None,
        project_id: ProjectId | None = None,
        kinds: Sequence[str] = (),
        since: datetime | None = None,
    ) -> Iterator[Event]: ...

    def tail(self, *, after_seq: int, kinds: Sequence[str] = (), limit: int = 1000
             ) -> list[tuple[int, Event]]: ...  # fmt: skip
    def last_seq(self) -> int: ...

    def blob(self, ref: BlobRef) -> Path: ...


class Emitter:
    """Appends events as one actor, stamped by the clock. `log=None` makes it a no-op;
    `best_effort` swallows a failing append (logging must not break the caller)."""

    def __init__(
        self, log: EventLog | None, clock: Clock, actor: str, *, best_effort: bool = False
    ) -> None:
        self._log = log
        self._clock = clock
        self._actor = actor
        self._best_effort = best_effort

    def build(
        self,
        kind: str,
        payload: dict | None = None,
        /,
        *,
        project_id: ProjectId | None = None,
        run_id: RunId | None = None,
        **fields: Any,
    ) -> Event:
        """The event to append; `fields` are merged after `payload` and override its keys."""
        return Event(
            id="",
            ts=self._clock.now(),
            kind=kind,
            actor=self._actor,
            project_id=project_id,
            run_id=run_id,
            payload={**(payload or {}), **fields},
            refs=[],
        )

    def __call__(
        self,
        kind: str,
        payload: dict | None = None,
        /,
        *,
        project_id: ProjectId | None = None,
        run_id: RunId | None = None,
        **fields: Any,
    ) -> None:
        """Append `build(...)`; a no-op without a log, and silent on failure if best_effort."""
        if self._log is None:
            return
        guard = contextlib.suppress(Exception) if self._best_effort else contextlib.nullcontext()
        with guard:
            self._log.append(
                self.build(kind, payload, project_id=project_id, run_id=run_id, **fields)
            )


def _ts_key(ts: datetime) -> str:
    """UTC, fixed-width so string comparison orders chronologically."""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


class SqliteEventLog:
    def __init__(self, db: Database, blobs: BlobStore, tz: tzinfo = SEOUL) -> None:
        self._db = db
        self._tz = tz
        self._blobs = blobs
        db.migrate("events_v1", _SCHEMA)

    def append(self, ev: Event) -> EventId:
        eid = ev.get("id") or new_id("evt")
        payload = ev["payload"]
        refs = list(ev["refs"])
        raw = json.dumps(payload, ensure_ascii=False, default=str)
        if len(raw.encode()) > MAX_PAYLOAD_BYTES:
            refs.append(self._blobs.put(raw.encode(), "json", permanent=True))
            payload = {"spilled": True, "preview": raw[:PREVIEW_CHARS]}
            raw = json.dumps(payload, ensure_ascii=False)
        self._db.execute(
            "INSERT INTO events(id, ts, kind, actor, project_id, run_id, payload_json, refs_json)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                eid,
                _ts_key(ev["ts"]),
                ev["kind"],
                ev["actor"],
                ev["project_id"],
                ev["run_id"],
                raw,
                json.dumps(refs),
            ),
        )
        return eid

    def query(
        self,
        *,
        run_id: RunId | None = None,
        project_id: ProjectId | None = None,
        kinds: Sequence[str] = (),
        since: datetime | None = None,
    ) -> Iterator[Event]:
        where, params = [], []
        if run_id is not None:
            where.append("run_id=?")
            params.append(run_id)
        if project_id is not None:
            where.append("project_id=?")
            params.append(project_id)
        if kinds:
            where.append(f"kind IN ({','.join('?' * len(kinds))})")
            params.extend(kinds)
        if since is not None:
            where.append("ts>=?")
            params.append(_ts_key(since))
        sql = "SELECT * FROM events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        rows = self._db.execute(sql + " ORDER BY seq", params).fetchall()
        for r in rows:
            yield _event(r, self._tz)

    def tail(
        self, *, after_seq: int, kinds: Sequence[str] = (), limit: int = 1000
    ) -> list[tuple[int, Event]]:
        """(seq, event) in log order after `after_seq`: a cursor that, unlike `since`, never
        skips an event appended later with an earlier timestamp."""
        sql, params = "SELECT * FROM events WHERE seq>?", [after_seq]
        if kinds:
            sql += f" AND kind IN ({','.join('?' * len(kinds))})"
            params.extend(kinds)
        rows = self._db.execute(sql + " ORDER BY seq LIMIT ?", [*params, limit]).fetchall()
        return [(r["seq"], _event(r, self._tz)) for r in rows]

    def last_seq(self) -> int:
        return self._db.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0]

    def blob(self, ref: BlobRef) -> Path:
        return self._blobs.path(ref)


def _event(r, tz: tzinfo) -> Event:
    return Event(
        id=r["id"],
        ts=datetime.strptime(r["ts"], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=UTC).astimezone(tz),
        kind=r["kind"],
        actor=r["actor"],
        project_id=r["project_id"],
        run_id=r["run_id"],
        payload=json.loads(r["payload_json"]),
        refs=json.loads(r["refs_json"]),
    )
