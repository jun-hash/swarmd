import sqlite3

import pytest

from swarm.store.db import Database


def _deferred_fk_db(tmp_path) -> Database:
    """A COMMIT that genuinely fails: a deferred foreign key is checked only at COMMIT."""
    d = Database(tmp_path / "fk.db")
    d.execute("PRAGMA foreign_keys=ON")
    d.migrate(
        "fk_v1",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY);"
        "CREATE TABLE child (pid INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    return d


def test_transaction_rolls_back_when_commit_fails(tmp_path):
    d = _deferred_fk_db(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        with d.transaction():
            d.execute("INSERT INTO child(pid) VALUES (42)")  # no parent 42 -> COMMIT fails
    assert d._conn.in_transaction is False
    assert d.execute("SELECT count(*) FROM child").fetchone()[0] == 0
    with d.transaction():  # the connection is usable again
        d.execute("INSERT INTO parent(id) VALUES (1)")
        d.execute("INSERT INTO child(pid) VALUES (1)")
    assert d.migrate("after_v1", "CREATE TABLE t (a INTEGER)") is True
    assert d.execute("SELECT count(*) FROM child").fetchone()[0] == 1


def test_migrate_rolls_back_when_commit_fails(tmp_path):
    d = _deferred_fk_db(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        d.migrate("bad_v1", "INSERT INTO child(pid) VALUES (7)")
    assert d._conn.in_transaction is False
    assert d.execute("SELECT 1 FROM _migrations WHERE name='bad_v1'").fetchone() is None
    with d.transaction():
        d.execute("INSERT INTO parent(id) VALUES (2)")
