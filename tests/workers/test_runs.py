"""RunTable: the runs table, its status machine and its row codecs."""

import asyncio
import contextlib
from datetime import timedelta

import pytest

from swarm.harness.base import Done
from swarm.store.events import Emitter
from swarm.workers.contract import IllegalTransition, RunFinished
from swarm.workers.runs import RunTable

from .conftest import BLOCK, V_OWN, add_evidence, order


@pytest.fixture
def runs(fakes) -> RunTable:
    return RunTable(fakes.db, Emitter(fakes.log, fakes.clock, "workers"), fakes.clock)


def spy_transactions(db, monkeypatch) -> list[tuple[str, int | None]]:
    """Every statement run on `db` as (first SQL word, number of the outermost transaction it
    ran in, or None outside one)."""
    seen: list[tuple[str, int | None]] = []
    state = {"depth": 0, "count": 0}
    transaction, execute = db.transaction, db.execute

    @contextlib.contextmanager
    def counted():
        if state["depth"] == 0:
            state["count"] += 1
        state["depth"] += 1
        try:
            with transaction():
                yield
        finally:
            state["depth"] -= 1

    def recorded(sql, params=()):
        seen.append((sql.split()[0], state["count"] if state["depth"] else None))
        return execute(sql, params)

    monkeypatch.setattr(db, "transaction", counted)
    monkeypatch.setattr(db, "execute", recorded)
    return seen


def test_runtable_update_refuses_a_finished_run_and_leaves_it_unchanged(fakes, runs, monkeypatch):
    runs.insert("run_1", order(), V_OWN, started=fakes.clock.now())
    runs.transition("run_1", "leased")
    seen = spy_transactions(fakes.db, monkeypatch)
    runs.update("run_1", summary="before")
    runs.transition("run_1", "failed")
    with pytest.raises(RunFinished):
        runs.update("run_1", summary="x")
    # each read-check-write is one transaction: the status read and its write never split
    assert [(sql, tx) for sql, tx in seen if sql in ("SELECT", "UPDATE")] == [
        ("SELECT", 1),
        ("UPDATE", 1),
        ("SELECT", 2),
        ("UPDATE", 2),
        ("SELECT", 3),
    ]
    assert runs.row("run_1")["summary"] == "before"
    assert runs.get("run_1")["status"] == "failed"
    assert fakes.kinds("run_1") == ["run.leased", "run.failed"]


def test_runtable_illegal_transition_raises(fakes, runs):
    runs.insert("run_1", order(), V_OWN, started=fakes.clock.now())
    with pytest.raises(IllegalTransition):
        runs.transition("run_1", "running")  # a new row only becomes leased
    runs.transition("run_1", "leased")
    with pytest.raises(IllegalTransition):
        runs.transition("run_1", "done")  # done only through verifying
    with pytest.raises(IllegalTransition):
        runs.transition("run_1", "bogus")
    with pytest.raises(KeyError):
        runs.transition("run_nope", "leased")
    assert runs.row("run_1")["status"] == "leased"


def test_runtable_interrupt_orphans_marks_leased_running_verifying(fakes, runs):
    statuses = {
        "run_a": ["leased"],
        "run_b": ["leased", "running"],
        "run_c": ["leased", "running", "verifying"],
        "run_d": ["leased", "running", "waiting_human"],
        "run_e": ["leased", "failed"],
    }
    for rid, path in statuses.items():
        runs.insert(rid, order(), V_OWN, started=fakes.clock.now())
        fakes.clock.advance(timedelta(seconds=1))
        for to in path:
            runs.transition(rid, to)
    assert runs.interrupt_orphans() == ["run_a", "run_b", "run_c"]
    got = {rid: runs.row(rid)["status"] for rid in statuses}
    assert got == {
        "run_a": "interrupted",
        "run_b": "interrupted",
        "run_c": "interrupted",
        "run_d": "waiting_human",
        "run_e": "failed",
    }


async def test_transition_table_rejects_illegal_moves(fakes, pool):
    fakes.harness.script = [add_evidence(fakes, pool), Done(summary="ok")]
    r = await pool.run(order(), V_OWN, lease=fakes.lease())
    for to in ("running", "failed", "interrupted", "cancelled"):
        with pytest.raises(IllegalTransition):
            pool.runs.transition(r["run_id"], to)
    assert pool.get(r["run_id"])["status"] == "done"
    fakes.harness.script = [BLOCK]
    fakes.harness.started.clear()
    task = asyncio.create_task(pool.run(order(), V_OWN, lease=fakes.lease()))
    await fakes.harness.started.wait()
    rid = pool.running()[0]["id"]
    with pytest.raises(IllegalTransition):
        pool.runs.transition(rid, "done")  # done only through verifying
    with pytest.raises(IllegalTransition):
        pool.runs.transition(rid, "bogus")
    await pool.cancel(rid, reason="t")
    await task
