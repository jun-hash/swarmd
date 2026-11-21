from datetime import datetime

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.projects.store import SqliteProjectStore
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import Project


# Overrides the root clock with the 09:00 start the store and browser packages also use.
@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))


@pytest.fixture
def home(tmp_path):
    return tmp_path / "swarm"


# Overrides the root log: the log has its own log.db, apart from the store's swarm.db.
@pytest.fixture
def log(tmp_path, clock):
    db = Database(tmp_path / "log.db")
    return SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))


@pytest.fixture
def store(db, home, log, clock):
    return SqliteProjectStore(db, log, home, clock)


def make_project(slug="compet", channel="telegram:-100123:42") -> Project:
    return Project(
        id="prj_" + slug,
        slug=slug,
        name="경쟁사",
        goal={
            "objective": "경쟁사 변화를 고객보다 먼저 안다",
            "key_results": [
                {
                    "id": "kr_1",
                    "metric": "주간 감지 건수",
                    "target": 5.0,
                    "current": 1.0,
                    "source": "log",
                }
            ],
        },
        strategy="",
        policies=[],
        autonomy=2,
        memory={"context": "C사 중심", "learnings": [], "failed_attempts": []},
        backlog=[],
        artifacts=[],
        channel=channel,
        board="compet",
        default_profile="work",
        default_variant={
            "harness": "own",
            "engine": "claude",
            "browser": "chrome",
            "context_policy": "default",
            "model": None,
        },
        weight=2,
        watches=[],
    )


@pytest.fixture
def project(store):
    return store.get(store.create(make_project()))
