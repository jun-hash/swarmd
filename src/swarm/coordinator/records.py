"""CoordinatorRecords: the Coordinator's durable state, every `coordinator_*` table.

Outcomes drained but not yet resumed, dispatched and queued orders, goal-change
confirmations, human events not yet handled, dropped worker lines already surfaced, the
per-item interrupt backoff and a key/value table (report dates, improve rotation). Every
statement on these tables lives here; callers own any transaction that spans several.
"""

import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime
from typing import NamedTuple

from swarm.clock import Clock
from swarm.ids import new_id
from swarm.store.codec import to_json
from swarm.store.db import Database
from swarm.store.kv import KvTable
from swarm.types import AskOutcome, ChannelRef, Event, ProjectId, RunId, Variant, WorkOrder

_SCHEMA = """
CREATE TABLE coordinator_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE coordinator_resumes (
  ask_id TEXT PRIMARY KEY, outcome_json TEXT NOT NULL, state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
CREATE TABLE coordinator_orders (
  order_id TEXT PRIMARY KEY, order_json TEXT NOT NULL, variant_json TEXT NOT NULL,
  origin TEXT NOT NULL, improve_task TEXT, state TEXT NOT NULL, run_id TEXT,
  channel TEXT, created_at TEXT NOT NULL);
CREATE TABLE coordinator_confirms (
  run_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, category TEXT NOT NULL,
  summary TEXT NOT NULL, text TEXT NOT NULL, ask_id TEXT, created_at TEXT NOT NULL);
CREATE TABLE coordinator_seen (event_id TEXT PRIMARY KEY)
"""
_SCHEMA_V2 = """
ALTER TABLE coordinator_resumes ADD COLUMN next_attempt_at TEXT
"""
# human events waiting for (or in) a respond turn: submitted human.* events, tells being
# classified and tells held after a Brain error. A restart reloads them, so a reply the
# human was promised ("받았어요, 정리해서 답할게요") is never lost; deleted once handled.
_SCHEMA_V3 = """
CREATE TABLE coordinator_held (
  id TEXT PRIMARY KEY, event_json TEXT NOT NULL, created_at TEXT NOT NULL)
"""
# consecutive non-quota interrupts per backlog item and when it may be dispatched again
_SCHEMA_V4 = """
CREATE TABLE coordinator_item_backoff (
  item_id TEXT PRIMARY KEY, failures INTEGER NOT NULL, not_before TEXT NOT NULL)
"""


class ResumeRow(NamedTuple):
    """A drained Ask outcome waiting to be resumed (or applied, for a confirmation)."""

    outcome: AskOutcome
    attempts: int
    next_attempt_at: datetime | None


def _dump_event(ev: Event) -> str:
    # not to_json: `default=str` writes a datetime as "date time", not isoformat's "dateTtime"
    return json.dumps({**ev, "ts": ev["ts"].isoformat()}, ensure_ascii=False, default=str)


def _load_event(raw: str) -> Event:
    ev = json.loads(raw)
    ev["ts"] = datetime.fromisoformat(ev["ts"])
    return ev


class CoordinatorRecords:
    def __init__(self, db: Database, clock: Clock) -> None:
        self._db = db
        self._clock = clock
        db.migrate("coordinator_v1", _SCHEMA)
        db.migrate("coordinator_v2", _SCHEMA_V2)
        db.migrate("coordinator_v3", _SCHEMA_V3)
        db.migrate("coordinator_v4", _SCHEMA_V4)
        self.kv = KvTable(db, "coordinator_kv")
        # resumes in flight when the process died: the run was left interrupted -> retry
        db.execute("UPDATE coordinator_resumes SET state='pending' WHERE state='inflight'")

    # --- held human events -------------------------------------------------------------

    def hold(self, ev: Event) -> Event:
        """Persist a human event until it is handled (survives a restart)."""
        ev = Event(**{**ev, "id": ev.get("id") or new_id("evt")})  # type: ignore[typeddict-item]
        self._db.execute(
            "INSERT OR REPLACE INTO coordinator_held(id, event_json, created_at) VALUES (?,?,?)",
            (ev["id"], _dump_event(ev), self._clock.now().isoformat()),
        )
        return ev

    def unhold(self, event_id: str) -> None:
        self._db.execute("DELETE FROM coordinator_held WHERE id=?", (event_id,))

    def held_events(self) -> list[Event]:
        rows = self._db.execute(
            "SELECT event_json FROM coordinator_held ORDER BY created_at, id"
        ).fetchall()
        return [_load_event(r["event_json"]) for r in rows]

    # --- resumes -----------------------------------------------------------------------

    def add_resumes(self, outcomes: Iterable[AskOutcome]) -> None:
        """Each outcome as a pending resume; the caller owns the transaction."""
        for o in outcomes:
            self._db.execute(
                "INSERT OR IGNORE INTO coordinator_resumes(ask_id, outcome_json, state,"
                " created_at) VALUES (?,?,'pending',?)",
                (o["ask_id"], to_json(o), self._clock.now().isoformat()),
            )

    def pending_resumes(self) -> list[ResumeRow]:
        rows = self._db.execute(
            "SELECT outcome_json, attempts, next_attempt_at FROM coordinator_resumes"
            " WHERE state='pending' ORDER BY created_at, ask_id"
        ).fetchall()
        return [
            ResumeRow(
                json.loads(r["outcome_json"]),
                r["attempts"],
                datetime.fromisoformat(r["next_attempt_at"]) if r["next_attempt_at"] else None,
            )
            for r in rows
        ]

    def resume_run_ids(self) -> set[RunId]:
        """Runs with an outcome on its way, in any state."""
        rows = self._db.execute("SELECT outcome_json FROM coordinator_resumes").fetchall()
        return {json.loads(r["outcome_json"])["run_id"] for r in rows}

    def mark_resume_inflight(self, ask_id: str) -> None:
        self._db.execute(
            "UPDATE coordinator_resumes SET state='inflight' WHERE ask_id=?", (ask_id,)
        )

    def resume_attempts(self, ask_id: str) -> int:
        row = self._db.execute(
            "SELECT attempts FROM coordinator_resumes WHERE ask_id=?", (ask_id,)
        ).fetchone()
        return row["attempts"] if row else 0

    def retry_resume(self, ask_id: str, attempts: int, at: datetime) -> None:
        self._db.execute(
            "UPDATE coordinator_resumes SET state='pending', attempts=?,"
            " next_attempt_at=? WHERE ask_id=?",
            (attempts, at.isoformat(), ask_id),
        )

    def drop_resume(self, ask_id: str) -> None:
        self._db.execute("DELETE FROM coordinator_resumes WHERE ask_id=?", (ask_id,))

    # --- orders ------------------------------------------------------------------------

    def queue_order(
        self, order: WorkOrder, variant: Variant, *, origin: str, channel: ChannelRef | None
    ) -> None:
        self._db.execute(
            "INSERT INTO coordinator_orders(order_id, order_json, variant_json, origin, state,"
            " channel, created_at) VALUES (?,?,?,?,'queued',?,?)",
            (
                order["id"], to_json(order), json.dumps(variant),
                origin, channel, self._clock.now().isoformat(),
            ),
        )  # fmt: skip

    def record_dispatch(
        self, order: WorkOrder, variant: Variant, *, origin: str, improve_task: str | None,
        channel: ChannelRef | None,
    ) -> None:  # fmt: skip
        self._db.execute(
            "INSERT OR REPLACE INTO coordinator_orders(order_id, order_json, variant_json,"
            " origin, improve_task, state, channel, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                order["id"], to_json(order), json.dumps(variant), origin, improve_task,
                "dispatched", channel, self._clock.now().isoformat(),
            ),
        )  # fmt: skip

    def order_row(self, order_id: str) -> sqlite3.Row | None:
        return self._db.execute(
            "SELECT * FROM coordinator_orders WHERE order_id=?", (order_id,)
        ).fetchone()

    def set_order(self, order_id: str, state: str, run_id: RunId | None = None) -> None:
        self._db.execute(
            "UPDATE coordinator_orders SET state=?, run_id=COALESCE(?, run_id) WHERE order_id=?",
            (state, run_id, order_id),
        )

    def orders_in(self, *states: str) -> list[sqlite3.Row]:
        """Orders in any of `states`, oldest first."""
        marks = ", ".join("?" * len(states))
        return self._db.execute(
            f"SELECT * FROM coordinator_orders WHERE state IN ({marks}) ORDER BY created_at",
            states,
        ).fetchall()

    def reserved_items(self) -> set[str]:
        """Backlog items a queued or held order already claims."""
        rows = self._db.execute(
            "SELECT order_json FROM coordinator_orders WHERE state IN ('queued', 'held')"
        ).fetchall()
        return {i for r in rows if (i := json.loads(r["order_json"]).get("backlog_item_id"))}

    # --- goal-change confirmations -----------------------------------------------------

    def add_confirm(
        self, run_id: RunId, project_id: ProjectId, category: str, summary: str, text: str
    ) -> None:
        self._db.execute(
            "INSERT INTO coordinator_confirms(run_id, project_id, category, summary, text,"
            " created_at) VALUES (?,?,?,?,?,?)",
            (run_id, project_id, category, summary, text, self._clock.now().isoformat()),
        )

    def confirm(self, run_id: RunId) -> sqlite3.Row | None:
        return self._db.execute(
            "SELECT * FROM coordinator_confirms WHERE run_id=?", (run_id,)
        ).fetchone()

    def confirm_project(self, run_id: RunId) -> ProjectId | None:
        r = self._db.execute(
            "SELECT project_id FROM coordinator_confirms WHERE run_id=?", (run_id,)
        ).fetchone()
        return r["project_id"] if r else None

    # --- dropped worker lines ----------------------------------------------------------

    def mark_seen(self, event_id: str) -> bool:
        """True the first time `event_id` is marked."""
        cur = self._db.execute(
            "INSERT OR IGNORE INTO coordinator_seen(event_id) VALUES (?)", (event_id,)
        )
        return bool(cur.rowcount)

    # --- per-item interrupt backoff ----------------------------------------------------

    def item_failures(self, item_id: str) -> int:
        row = self._db.execute(
            "SELECT failures FROM coordinator_item_backoff WHERE item_id=?", (item_id,)
        ).fetchone()
        return row["failures"] if row else 0

    def back_off_item(self, item_id: str, failures: int, not_before: datetime) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO coordinator_item_backoff(item_id, failures, not_before)"
            " VALUES (?,?,?)",
            (item_id, failures, not_before.isoformat()),
        )

    def clear_item(self, item_id: str) -> None:
        self._db.execute("DELETE FROM coordinator_item_backoff WHERE item_id=?", (item_id,))

    def backoff_items(self, now: datetime) -> set[str]:
        """Items that may not be dispatched before their not_before."""
        rows = self._db.execute(
            "SELECT item_id FROM coordinator_item_backoff WHERE not_before > ?", (now.isoformat(),)
        ).fetchall()
        return {r["item_id"] for r in rows}
