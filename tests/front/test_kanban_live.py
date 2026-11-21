r"""Live check of HermesKanbanMirror against a real `hermes dashboard` (open question:
plugin REST auth, payload shapes, WS events). Skipped unless SWARM_HERMES_LIVE=1.

Run on the machine that runs Hermes:
    hermes dashboard --no-open &
    SWARM_HERMES_LIVE=1 [SWARM_HERMES_URL=http://127.0.0.1:9119] \
      [SWARM_HERMES_TOKEN=<session-token>] \
      uv run pytest -q -m hermes tests/front/test_kanban_live.py
It writes to the board `swarm-probe` only.
"""

import asyncio
import os
from datetime import datetime

import pytest

from swarm.clock import SEOUL, SystemClock
from swarm.front.kanban import HermesKanbanMirror, HermesTransport
from swarm.projects.store import SqliteProjectStore
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import CardView, Project

LIVE = os.environ.get("SWARM_HERMES_LIVE") == "1"
URL = os.environ.get("SWARM_HERMES_URL", "http://127.0.0.1:9119")
TOKEN = os.environ.get("SWARM_HERMES_TOKEN") or None
BOARD = "swarm-probe"

pytestmark = [
    pytest.mark.hermes,
    pytest.mark.skipif(
        not LIVE, reason="needs a running `hermes dashboard`; set SWARM_HERMES_LIVE=1 to run"
    ),
]


def probe_project() -> Project:
    return Project(
        id="prj_probe", slug=BOARD, name="probe",
        goal={"objective": "probe", "key_results": []}, strategy="", policies=[], autonomy=1,
        memory={"context": "", "learnings": [], "failed_attempts": []}, backlog=[],
        artifacts=[], channel="telegram:-1:1", board=BOARD, default_profile="work",
        default_variant={"harness": "own", "engine": "claude", "browser": "chrome",
                         "context_policy": "default", "model": None},
        weight=1, watches=[],
    )  # fmt: skip


async def test_live_create_update_card(tmp_path):
    clock = SystemClock()
    db = Database(tmp_path / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    projects.create(probe_project())
    mirror = HermesKanbanMirror(URL, db, log, clock, projects=projects, token=TOKEN)
    key = "bl_probe_" + datetime.now(SEOUL).strftime("%H%M%S")
    card = CardView(key=key, board=BOARD, title="swarm probe", body="live test",
                    status="ready", priority=1, links=[], attachments=[])  # fmt: skip
    stop = asyncio.Event()
    listener = asyncio.create_task(mirror.listen(stop))
    try:
        mirror.upsert(card)
        assert await mirror.flush() == 1, [e["payload"] for e in log.query()]
        mirror.upsert({**card, "status": "running"})  # type: ignore[typeddict-item]
        assert await mirror.flush() == 1
        await asyncio.sleep(2)  # let the WS echoes arrive
        assert list(mirror.inbound()) == []  # our own writes never come back as changes
        t = HermesTransport(URL, token=TOKEN)
        try:
            tasks = await t.list_tasks(BOARD)
        finally:
            await t.aclose()
        mine = [x for x in tasks if x.status == "running"]
        assert mine, tasks
    finally:
        stop.set()
        await asyncio.wait_for(listener, 5)
        await mirror.aclose()
