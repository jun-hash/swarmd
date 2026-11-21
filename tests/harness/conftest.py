import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog

from .helpers import FakeInternal, Site

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


@pytest.fixture
def site():
    s = Site().start()
    yield s
    s.stop()


@pytest.fixture
def fake_internal():
    f = FakeInternal().start()
    yield f
    f.stop()


@pytest.fixture
async def chrome(tmp_path):
    from swarm.browser.chrome import ChromeRuntime

    if not CHROME.exists():
        pytest.skip("Google Chrome not installed")
    clock = FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))
    # blobs0 keeps the runtime's blobs apart from the MCP server's tmp_path/blobs, which tests read.
    log = SqliteEventLog(Database(tmp_path / "swarm.db"), BlobStore(tmp_path / "blobs0", clock))
    dirs = {"test": Path(tempfile.mkdtemp(prefix="swarm-mcp-"))}
    rt = ChromeRuntime(CHROME, dirs, log, headless=True, reconnect_delays=(0.1, 0.1, 0.1))
    yield rt
    await rt.shutdown()
    shutil.rmtree(dirs["test"], ignore_errors=True)


@pytest.fixture
async def chrome_session(chrome):
    s = await chrome.open_session(profile="test", purpose="mcp-test")
    yield s
    await chrome.close_session(s["id"])
