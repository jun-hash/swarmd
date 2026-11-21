import json
from datetime import timedelta

from swarm.clock import SEOUL
from swarm.ids import new_id
from swarm.store.db import Database


def ev(kind, *, run_id=None, project_id=None, payload=None, ts=None, clock=None):
    from datetime import datetime

    return {
        "id": new_id("evt"),
        "ts": ts or datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL),
        "kind": kind,
        "actor": "test",
        "project_id": project_id,
        "run_id": run_id,
        "payload": payload or {},
        "refs": [],
    }


def test_append_then_query_in_order(log):
    ids = [
        log.append(ev("run.started", run_id="run_1")),
        log.append(ev("run.tool_result", run_id="run_1")),
    ]
    log.append(ev("run.started", run_id="run_2"))
    assert [e["id"] for e in log.query(run_id="run_1")] == ids


def test_no_update_or_delete_api(log):
    assert not hasattr(log, "update") and not hasattr(log, "delete")


def test_large_payload_spills_to_blob(log):
    log.append(ev("run.tool_result", payload={"text": "x" * 20_000}))
    e = next(log.query(kinds=["run.tool_result"]))
    assert len(json.dumps(e["payload"])) <= 16_384
    assert any(r.startswith("blob_") for r in e["refs"])
    assert e["payload"]["spilled"] is True and len(e["payload"]["preview"]) == 1000
    assert json.loads(log.blob(e["refs"][-1]).read_text())["text"] == "x" * 20_000


def test_query_filters_and_roundtrip(log):
    t0 = ev("a.b", project_id="p1")["ts"]
    log.append(ev("a.b", project_id="p1", ts=t0, payload={"k": "한글"}))
    log.append(ev("c.d", project_id="p2", ts=t0 + timedelta(hours=2)))
    assert len(list(log.query(project_id="p1"))) == 1
    assert len(list(log.query(kinds=["c.d"]))) == 1
    got = list(log.query(since=t0 + timedelta(hours=1)))
    assert [e["kind"] for e in got] == ["c.d"]
    first = next(log.query(project_id="p1"))
    assert first["payload"] == {"k": "한글"} and first["ts"] == t0


def test_migrate_is_idempotent_and_wal(tmp_path):
    d = Database(tmp_path / "x.db")
    sql = "CREATE TABLE t (a INTEGER)"
    d.migrate("t1", sql)
    d.migrate("t1", sql)  # no error, not re-applied
    d2 = Database(tmp_path / "x.db")
    d2.migrate("t1", sql)
    assert d.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert d.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_migrate_race_across_connections(tmp_path):
    a, b = Database(tmp_path / "r.db"), Database(tmp_path / "r.db")
    sql = "CREATE TABLE t (a INTEGER)"
    assert a.migrate("t1", sql) is True
    assert b.migrate("t1", sql) is False
    n = a.execute("SELECT count(*) FROM _migrations WHERE name='t1'").fetchone()[0]
    assert n == 1


def test_spilled_payload_survives_prune(log, blobs, fake_clock):
    log.append(ev("run.tool_result", payload={"text": "x" * 20_000}))
    e = next(log.query())
    ref = e["refs"][-1]
    fake_clock.advance(timedelta(days=31))
    blobs.prune(fake_clock.now(), keep_days=30)
    assert log.blob(ref).exists()


def test_tail_follows_log_order_after_a_seq(log):
    from datetime import datetime

    late = datetime(2026, 11, 6, 8, 0, tzinfo=SEOUL)  # appended last, stamped earliest
    a = log.append(ev("run.started"))
    log.append(ev("project.created"))
    b = log.append(ev("run.finished", ts=late))
    assert log.last_seq() == 3
    got = log.tail(after_seq=0, kinds=["run.started", "run.finished"])
    assert [(seq, e["id"]) for seq, e in got] == [(1, a), (3, b)]
    assert [seq for seq, _ in log.tail(after_seq=1)] == [2, 3]
    assert log.tail(after_seq=3) == []
