"""A string key/value table owned by one component (its migration stays with the owner)."""

import json
import re
from typing import Any

from swarm.store.db import Database

_TABLE = re.compile(r"[a-z_]+")


class KvTable:
    def __init__(self, db: Database, table: str) -> None:
        if not _TABLE.fullmatch(table):
            raise ValueError(f"invalid table name: {table!r}")
        self._db = db
        self._table = table

    def get(self, key: str) -> str | None:
        r = self._db.execute(f"SELECT value FROM {self._table} WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def set(self, key: str, value: str | None) -> None:
        """`None` deletes the row."""
        if value is None:
            self._db.execute(f"DELETE FROM {self._table} WHERE key=?", (key,))
        else:
            self._db.execute(
                f"INSERT OR REPLACE INTO {self._table}(key, value) VALUES (?,?)", (key, value)
            )

    def get_json(self, key: str, default: Any = None) -> Any:
        """`default` only when the key is missing; a stored `null` comes back as `None`."""
        raw = self.get(key)
        return default if raw is None else json.loads(raw)

    def set_json(self, key: str, value: Any) -> None:
        """Always stores the encoding, so `None` is kept as `null` and never deletes."""
        self.set(key, json.dumps(value, ensure_ascii=False))
