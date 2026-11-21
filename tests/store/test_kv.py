import pytest

from swarm.store.kv import KvTable


@pytest.fixture
def kv(db):
    db.migrate("t_kv_v1", "CREATE TABLE t_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    return KvTable(db, "t_kv")


def test_kv_set_get_and_overwrite(kv):
    assert kv.get("k") is None
    kv.set("k", "a")
    kv.set("k", "b")
    assert kv.get("k") == "b"


def test_kv_none_deletes(kv):
    kv.set("k", "a")
    kv.set("k", None)
    assert kv.get("k") is None


def test_kv_json_roundtrip_keeps_null(kv, db):
    kv.set_json("k", None)
    assert db.execute("SELECT value FROM t_kv WHERE key='k'").fetchone()["value"] == "null"
    assert kv.get_json("k", 5) is None  # a stored null is not a missing key
    assert kv.get_json("missing", 5) == 5


def test_kv_json_keeps_non_ascii_text(kv):
    kv.set_json("k", {"t": "한"})
    assert kv.get("k") == '{"t": "한"}'


def test_kv_rejects_a_table_name_that_is_not_an_identifier(db):
    with pytest.raises(ValueError):
        KvTable(db, "t; DROP TABLE x")
