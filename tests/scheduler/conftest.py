import pytest

from swarm.scheduler.ledger import SqliteQuotaLedger


@pytest.fixture
def ledger(db, log, clock):
    return SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
