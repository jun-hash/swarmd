"""CoordinatorRecords: every coordinator_* table behind one class."""

import sqlite3
from datetime import datetime, timedelta

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.coordinator.records import CoordinatorRecords, ResumeRow
from swarm.inbox.inbox import SqliteInbox
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import AskDraft, AskOutcome

NOW = datetime(2026, 11, 5, 3, 0, tzinfo=SEOUL)
GENERAL = "telegram:-100123:1"


class NoProjects:
    def get(self, project_id):
        raise KeyError(project_id)


class NoBrain:
    async def decide(self, **kw):
        raise AssertionError("no policy to match")


class FailOnSecondResume:
    """The database, except that the second resume insert raises."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self.inserts = 0

    def __getattr__(self, name):
        return getattr(self._db, name)

    def execute(self, sql, params=()):
        if sql.startswith("INSERT OR IGNORE INTO coordinator_resumes"):
            self.inserts += 1
            if self.inserts == 2:
                raise sqlite3.OperationalError("disk I/O error")
        return self._db.execute(sql, params)


# Overrides the root clock: tests here count from the module's NOW.
@pytest.fixture
def clock():
    return FakeClock(NOW)


@pytest.fixture
def records(db, clock):
    return CoordinatorRecords(db, clock)


def outcome(ask_id: str, run_id: str = "run_1") -> AskOutcome:
    return AskOutcome(
        ask_id=ask_id, run_id=run_id, status="answered", choice="A", raw_text="A", via="test"
    )


async def test_drain_and_resume_insert_roll_back_together(db, clock, tmp_path):
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    inbox = SqliteInbox(
        db, log, NoProjects(), NoBrain(), clock,
        run_project=lambda r: None, general_channel=GENERAL,
    )  # fmt: skip
    for run_id in ("run_1", "run_2"):
        t = await inbox.raise_ask(
            run_id,
            AskDraft(kind="decision", question="어느 쪽?", options=["A", "B"], recommended="A",
                     default_after=None, irreversible=False, evidence=[]),
        )  # fmt: skip
        await inbox.answer(t["ask_id"], choice="A", raw_text="A", via=GENERAL)
    flaky = FailOnSecondResume(db)
    records = CoordinatorRecords(flaky, clock)  # type: ignore[arg-type]
    with pytest.raises(sqlite3.OperationalError), db.transaction():
        records.add_resumes(inbox.drain_closed())
    assert flaky.inserts == 2  # the first insert ran, and was rolled back with the drain
    assert records.pending_resumes() == []
    assert [o["run_id"] for o in inbox.drain_closed()] == ["run_1", "run_2"]


def test_retry_resume_sets_pending_attempts_and_due(records, clock):
    o = outcome("ask_1")
    records.add_resumes([o])
    assert records.pending_resumes() == [ResumeRow(o, 0, None)]
    records.mark_resume_inflight("ask_1")
    assert records.pending_resumes() == []
    later = clock.now() + timedelta(minutes=5)
    records.retry_resume("ask_1", 2, later)
    assert records.pending_resumes() == [ResumeRow(o, 2, later)]
    assert records.resume_attempts("ask_1") == 2
    assert records.resume_run_ids() == {"run_1"}
    records.drop_resume("ask_1")
    assert records.resume_attempts("ask_1") == 0
    assert records.resume_run_ids() == set()


def test_mark_seen_true_once(records):
    assert records.mark_seen("evt_1") is True
    assert records.mark_seen("evt_1") is False
    assert records.mark_seen("evt_2") is True


def test_backoff_items_only_future(records, clock):
    now = clock.now()
    records.back_off_item("bl_future", 1, now + timedelta(seconds=60))
    records.back_off_item("bl_now", 2, now)
    records.back_off_item("bl_past", 1, now - timedelta(seconds=1))
    assert records.backoff_items(now) == {"bl_future"}
    assert records.item_failures("bl_now") == 2
    records.clear_item("bl_now")
    assert records.item_failures("bl_now") == 0
