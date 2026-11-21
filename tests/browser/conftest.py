import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog

from .guard_server import GuardServer

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


# Overrides the root log with one on its own 09:00 clock.
@pytest.fixture
def log(tmp_path):
    clock = FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))
    return SqliteEventLog(Database(tmp_path / "swarm.db"), BlobStore(tmp_path / "blobs", clock))


@pytest.fixture
def profile_dirs():
    # short paths under the system tmp (Chrome's singleton socket path has a length limit)
    dirs = {name: Path(tempfile.mkdtemp(prefix=f"swarm-{name}-")) for name in ("test", "other")}
    yield dirs
    for d in dirs.values():
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
async def chrome(profile_dirs, log):
    from swarm.browser.chrome import ChromeRuntime

    if not CHROME.exists():
        pytest.skip("Google Chrome not installed")
    rt = ChromeRuntime(CHROME, profile_dirs, log, headless=True, reconnect_delays=(0.1, 0.1, 0.1))
    yield rt
    await rt.shutdown()


@pytest.fixture
def guard_server():
    server = GuardServer().start()
    yield server
    server.stop()


@pytest.fixture
async def guard(chrome, guard_server):
    g = await chrome.open_guard("test", allowlist=["localhost"])
    yield g
    await g.close()
