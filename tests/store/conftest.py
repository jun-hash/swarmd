from datetime import datetime

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.store.blobs import BlobStore
from swarm.store.events import SqliteEventLog


@pytest.fixture
def fake_clock():
    return FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))


@pytest.fixture
def blobs(tmp_path, fake_clock):
    return BlobStore(tmp_path / "blobs", fake_clock)


# Overrides the root log: it writes into the `blobs` fixture, so tests read the same blob store.
@pytest.fixture
def log(db, blobs):
    return SqliteEventLog(db, blobs)
