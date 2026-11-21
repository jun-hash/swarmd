"""SQLite connection (WAL, busy_timeout) and idempotent named migrations."""

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self._tx_depth = 0
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS _migrations "
            "(name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """BEGIN IMMEDIATE ... COMMIT, ROLLBACK on error. Re-entrant: inner calls join the outer."""
        with self._lock:
            if self._tx_depth:
                yield
                return
            self._conn.execute("BEGIN IMMEDIATE")
            self._tx_depth = 1
            try:
                yield
                self._conn.execute("COMMIT")
            except BaseException:
                # Also reached when COMMIT itself fails (disk full, deferred constraint): without
                # this the connection stays inside an open tx and every later call fails.
                self._rollback_quietly()
                raise
            finally:
                self._tx_depth = 0

    def _rollback_quietly(self) -> None:
        if self._conn.in_transaction:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    def migrate(self, name: str, sql: str) -> bool:
        """Apply `sql` once under `name`. Returns True if applied, False if already applied."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if self._conn.execute("SELECT 1 FROM _migrations WHERE name=?", (name,)).fetchone():
                    self._conn.execute("ROLLBACK")
                    return False
                for stmt in sql.split(";"):
                    if stmt.strip():
                        self._conn.execute(stmt)
                self._conn.execute(
                    "INSERT INTO _migrations(name, applied_at) VALUES (?, datetime('now'))", (name,)
                )
                n = self._conn.execute("SELECT count(*) FROM _migrations").fetchone()[0]
                self._conn.execute(f"PRAGMA user_version={int(n)}")
                self._conn.execute("COMMIT")
            except BaseException:
                self._rollback_quietly()
                raise
            return True

    def close(self) -> None:
        with self._lock:
            self._conn.close()
