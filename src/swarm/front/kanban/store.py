"""MirrorTables: the kanban mirror's durable state, every `kanban_*` table.

Cards (the desired state, the dirty queue and what we last pushed), the echoes of our own
writes, our queued comments, the human comments already handled, recent reverts, card
extras (summary, links, attachments), tasks left on a board a card moved away from, and a
key/value table (backoff, feed and WS positions). Every statement on these tables lives
here; callers own any transaction that spans several.
"""

import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

from swarm.store.db import Database
from swarm.store.kv import KvTable
from swarm.types import CardView

_SCHEMA = """
CREATE TABLE kanban_cards (
  key TEXT PRIMARY KEY, board TEXT NOT NULL, task_id TEXT,
  card_json TEXT NOT NULL, status TEXT NOT NULL, priority INTEGER NOT NULL,
  dirty INTEGER NOT NULL DEFAULT 1, dirty_seq INTEGER NOT NULL DEFAULT 0,
  dead INTEGER NOT NULL DEFAULT 0, last_error TEXT, done_at TEXT,
  proposal_json TEXT, proposal_at TEXT, updated_at TEXT NOT NULL);
CREATE INDEX kanban_cards_dirty ON kanban_cards(dirty, dirty_seq);
CREATE INDEX kanban_cards_task ON kanban_cards(task_id);
CREATE TABLE kanban_echoes (
  task_id TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL, at TEXT NOT NULL,
  PRIMARY KEY (task_id, status, updated_at));
CREATE TABLE kanban_comments (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, ref TEXT NOT NULL UNIQUE, key TEXT NOT NULL,
  text TEXT NOT NULL, status TEXT NOT NULL, remote_id TEXT, last_error TEXT,
  created_at TEXT NOT NULL);
CREATE TABLE kanban_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)
"""
# the last pushed state per card; the echo key becomes (task, status+priority, instant)
_SCHEMA_V2 = """
ALTER TABLE kanban_cards ADD COLUMN pushed_json TEXT;
DROP TABLE kanban_echoes;
CREATE TABLE kanban_echoes (
  task_id TEXT NOT NULL, state TEXT NOT NULL, updated_at TEXT NOT NULL, at TEXT NOT NULL,
  PRIMARY KEY (task_id, state, updated_at))
"""
_SCHEMA_V3 = """
CREATE TABLE kanban_reverts (key TEXT NOT NULL, at TEXT NOT NULL);
CREATE INDEX kanban_reverts_key ON kanban_reverts(key, at);
ALTER TABLE kanban_cards ADD COLUMN contested INTEGER NOT NULL DEFAULT 0
"""
_SCHEMA_V4 = """
CREATE TABLE kanban_seen_comments (
  task_id TEXT NOT NULL, comment_key TEXT NOT NULL, at TEXT NOT NULL,
  PRIMARY KEY (task_id, comment_key))
"""
_SCHEMA_V5 = """
CREATE TABLE kanban_extras (
  key TEXT PRIMARY KEY, summary TEXT NOT NULL DEFAULT '',
  links_json TEXT NOT NULL DEFAULT '[]', attachments_json TEXT NOT NULL DEFAULT '[]')
"""
_SCHEMA_V6 = """
CREATE TABLE kanban_orphans (
  task_id TEXT PRIMARY KEY, board TEXT NOT NULL, at TEXT NOT NULL)
"""


class MirrorTables:
    """Every read and write of the mirror's `kanban_*` tables, migrated on construction;
    used only by the mirror, which owns any transaction spanning several statements."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self.kv = KvTable(db, "kanban_kv")
        db.migrate("front_kanban_v1", _SCHEMA)
        db.migrate("front_kanban_v2", _SCHEMA_V2)
        db.migrate("front_kanban_v3", _SCHEMA_V3)
        db.migrate("front_kanban_v4", _SCHEMA_V4)
        db.migrate("front_kanban_v5", _SCHEMA_V5)
        db.migrate("front_kanban_v6", _SCHEMA_V6)

    # --- cards ----------------------------------------------------------------------------

    def card(self, key: str) -> sqlite3.Row | None:
        """The card row of `key`."""
        return self._db.execute("SELECT * FROM kanban_cards WHERE key=?", (key,)).fetchone()

    def card_by_task(self, task_id: str) -> sqlite3.Row | None:
        """The card row shown as Hermes task `task_id`."""
        return self._db.execute("SELECT * FROM kanban_cards WHERE task_id=?", (task_id,)).fetchone()

    def key_of_task(self, task_id: str) -> str | None:
        """The key of the card shown as Hermes task `task_id`."""
        row = self._db.execute(
            "SELECT key FROM kanban_cards WHERE task_id=?", (task_id,)
        ).fetchone()
        return row["key"] if row else None

    def next_seq(self) -> int:
        """The queue position after every card queued so far."""
        return self._db.execute(
            "SELECT COALESCE(MAX(dirty_seq), 0) + 1 FROM kanban_cards"
        ).fetchone()[0]

    def add_card(
        self, key: str, board: str, card_json: str, status: str, priority: int,
        done_at: str | None, at: str,
    ) -> None:  # fmt: skip
        """A new card, queued at the end."""
        self._db.execute(
            "INSERT INTO kanban_cards(key, board, card_json, status, priority, dirty,"
            " dirty_seq, done_at, updated_at) VALUES (?,?,?,?,?,1,?,?,?)",
            (key, board, card_json, status, priority, self.next_seq(), done_at, at),
        )

    def put_card(
        self, key: str, board: str, card_json: str, status: str, priority: int,
        done_at: str | None, at: str, task_id: str | None,
    ) -> None:  # fmt: skip
        """A new desired state for an existing card; clears its error and contested flag.
        The caller queues it with `mark_dirty`."""
        self._db.execute(
            "UPDATE kanban_cards SET board=?, card_json=?, status=?, priority=?,"
            " done_at=?, updated_at=?, task_id=?, last_error=NULL, contested=0 WHERE key=?",
            (board, card_json, status, priority, done_at, at, task_id, key),
        )

    def mark_dirty(self, key: str) -> None:
        """Queue `key` at the end (again), revived if dead, its open proposal dropped."""
        self._db.execute(
            "UPDATE kanban_cards SET dirty=1, dead=0, dirty_seq=?, proposal_json=NULL,"
            " proposal_at=NULL WHERE key=?",
            (self.next_seq(), key),
        )

    def dirty_cards(self) -> list[sqlite3.Row]:
        """Queued cards in queue order."""
        return self._db.execute(
            "SELECT * FROM kanban_cards WHERE dirty=1 ORDER BY dirty_seq"
        ).fetchall()

    def dirty_count(self) -> int:
        """How many cards are queued."""
        return self._db.execute("SELECT count(*) FROM kanban_cards WHERE dirty=1").fetchone()[0]

    def mark_dead(self, key: str, dirty_seq: int, error: str) -> None:
        """Dead-letter `key` if it was not queued again since `dirty_seq` was read."""
        self._db.execute(
            "UPDATE kanban_cards SET dead=1, dirty=0, last_error=? WHERE key=? AND dirty_seq=?",
            (error, key, dirty_seq),
        )

    def set_pushed(self, key: str, dirty_seq: int, task_id: str, pushed: dict) -> None:
        """Record a write Hermes accepted: the task id and the state it holds now. The card
        leaves the queue unless it was queued again since `dirty_seq` was read."""
        self._db.execute(
            "UPDATE kanban_cards SET task_id=?, last_error=NULL, pushed_json=?,"
            " dirty=CASE WHEN dirty_seq=? THEN 0 ELSE dirty END WHERE key=?",
            (task_id, json.dumps(pushed, sort_keys=True), dirty_seq, key),
        )

    def forget_task(self, key: str) -> None:
        """`key`'s task is gone from the board: the next push creates a new one."""
        self._db.execute("UPDATE kanban_cards SET task_id=NULL WHERE key=?", (key,))

    def set_contested(self, key: str, contested: bool) -> None:
        """Mark or clear `key` as fought over by another writer."""
        self._db.execute("UPDATE kanban_cards SET contested=? WHERE key=?", (int(contested), key))

    def set_proposal(self, key: str, proposal_json: str, at: str) -> None:
        """The remote state proposed to the Coordinator for `key`, and when."""
        self._db.execute(
            "UPDATE kanban_cards SET proposal_json=?, proposal_at=? WHERE key=?",
            (proposal_json, at, key),
        )

    def board_cards(self, board: str) -> list[sqlite3.Row]:
        """Cards of `board` that have a task on it."""
        return self._db.execute(
            "SELECT * FROM kanban_cards WHERE board=? AND task_id IS NOT NULL", (board,)
        ).fetchall()

    def done_cards(self, board: str) -> list[CardView]:
        """The stored cards of `board` whose status is done."""
        return [
            json.loads(r["card_json"])
            for r in self._db.execute(
                "SELECT card_json FROM kanban_cards WHERE board=? AND status='done'", (board,)
            ).fetchall()
        ]

    # --- extras ---------------------------------------------------------------------------

    def extras(self, key: str) -> dict:
        """Summary, links and attachments shown on `key`'s card (empty when none)."""
        r = self._db.execute("SELECT * FROM kanban_extras WHERE key=?", (key,)).fetchone()
        if r is None:
            return {"summary": "", "links": [], "attachments": []}
        return {
            "summary": r["summary"],
            "links": json.loads(r["links_json"]),
            "attachments": [Path(a) for a in json.loads(r["attachments_json"])],
        }

    def set_extras(
        self,
        key: str,
        *,
        summary: str,
        links: Sequence[str] | None = None,
        attachments: Sequence[Path] | None = None,
    ) -> None:
        """Replace `key`'s summary; links / attachments only when given."""
        cur = self.extras(key)
        links = cur["links"] if links is None else list(links)
        attachments = cur["attachments"] if attachments is None else list(attachments)
        self._db.execute(
            "INSERT OR REPLACE INTO kanban_extras(key, summary, links_json, attachments_json)"
            " VALUES (?,?,?,?)",
            (key, summary, json.dumps(links), json.dumps([str(a) for a in attachments])),
        )

    # --- echoes ---------------------------------------------------------------------------

    def record_echo(self, task_id: str, state: str, updated_at: str, at: str) -> None:
        """The state a write of ours left on `task_id`, to recognise when it comes back."""
        self._db.execute(
            "INSERT OR REPLACE INTO kanban_echoes(task_id, state, updated_at, at) VALUES (?,?,?,?)",
            (task_id, state, updated_at, at),
        )

    def take_echo(self, task_id: str, state: str, updated_at: str) -> bool:
        """Delete the echo matching this remote state (either updated_at may be unknown).
        True if there was one: the state is our own write coming back."""
        hit = self._db.execute(
            "DELETE FROM kanban_echoes WHERE task_id=? AND state=?"
            " AND (updated_at=? OR updated_at='' OR ?='')",
            (task_id, state, updated_at, updated_at),
        )
        return bool(hit.rowcount)

    def prune_echoes(self, before: str) -> None:
        """Forget echoes recorded before `before`."""
        self._db.execute("DELETE FROM kanban_echoes WHERE at < ?", (before,))

    # --- reverts --------------------------------------------------------------------------

    def prune_reverts(self, before: str) -> None:
        """Forget reverts made before `before`."""
        self._db.execute("DELETE FROM kanban_reverts WHERE at < ?", (before,))

    def revert_count(self, key: str) -> int:
        """Reverts of `key` still remembered."""
        return self._db.execute(
            "SELECT count(*) FROM kanban_reverts WHERE key=?", (key,)
        ).fetchone()[0]

    def record_revert(self, key: str, at: str) -> None:
        """We put `key`'s canonical card back at `at`."""
        self._db.execute("INSERT INTO kanban_reverts(key, at) VALUES (?,?)", (key, at))

    # --- comments -------------------------------------------------------------------------

    def add_comment(self, ref: str, key: str, text: str, at: str) -> bool:
        """Queue a comment of ours; False if `ref` was queued before."""
        cur = self._db.execute(
            "INSERT OR IGNORE INTO kanban_comments(ref, key, text, status, created_at)"
            " VALUES (?,?,?,'pending',?)",
            (ref, key, text, at),
        )
        return bool(cur.rowcount)

    def pending_comments(self) -> list[sqlite3.Row]:
        """Our queued comments in queue order."""
        return self._db.execute(
            "SELECT * FROM kanban_comments WHERE status='pending' ORDER BY seq"
        ).fetchall()

    def comment_dead(self, seq: int, error: str) -> None:
        """Hermes refused comment `seq`: dead-letter it."""
        self._db.execute(
            "UPDATE kanban_comments SET status='dead', last_error=? WHERE seq=?", (error, seq)
        )

    def comment_sent(self, seq: int, remote_id: str | None) -> None:
        """Comment `seq` is on the board as `remote_id`."""
        self._db.execute(
            "UPDATE kanban_comments SET status='sent', remote_id=? WHERE seq=?", (remote_id, seq)
        )

    def is_our_comment(self, remote_id: str, key: str, text: str) -> bool:
        """A board comment is ours: its remote id, or `key` and the exact text, match."""
        return (
            self._db.execute(
                "SELECT 1 FROM kanban_comments WHERE (remote_id IS NOT NULL AND remote_id=?)"
                " OR (key=? AND text=?) LIMIT 1",
                (remote_id, key, text),
            ).fetchone()
            is not None
        )

    def mark_comment_seen(self, task_id: str, comment_key: str, at: str) -> bool:
        """Remember a human comment as handled; True the first time it is seen."""
        first = self._db.execute(
            "INSERT OR IGNORE INTO kanban_seen_comments(task_id, comment_key, at) VALUES (?,?,?)",
            (task_id, comment_key, at),
        )
        return bool(first.rowcount)

    # --- orphans --------------------------------------------------------------------------

    def add_orphan(self, task_id: str, board: str, at: str) -> None:
        """A task left on `board` by a card that moved to another board."""
        self._db.execute(
            "INSERT OR REPLACE INTO kanban_orphans(task_id, board, at) VALUES (?,?,?)",
            (task_id, board, at),
        )

    def orphans(self) -> list[sqlite3.Row]:
        """Tasks still to archive on an old board, oldest first."""
        return self._db.execute("SELECT * FROM kanban_orphans ORDER BY at").fetchall()

    def drop_orphan(self, task_id: str) -> None:
        """`task_id` needs no more archiving."""
        self._db.execute("DELETE FROM kanban_orphans WHERE task_id=?", (task_id,))
