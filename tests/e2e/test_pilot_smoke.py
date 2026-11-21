"""Pilot smoke: the whole swarmd, in process, on real parts. That means
headless Chrome, the real `claude` CLI (haiku, subscription login) for Brain, worker and
Verifier, the own harness, and the SwarmFront MCP over HTTP with the bearer. The pilot
project is created with the real `swarm project create`. It drives a local static
"competitor pricing page" through `swarm_tell`, exactly as Hermes would. The only stand-ins
are Telegram (a mock Bot API, so nothing is really sent) and, without a live Hermes, the
kanban dashboard (down).

    uv run pytest -q -m integration tests/e2e/test_pilot_smoke.py -s
    # with a live Hermes dashboard, the kanban card check runs too:
    SWARM_HERMES_LIVE=1 [SWARM_HERMES_URL=...] [SWARM_HERMES_TOKEN=...] uv run pytest ...

Spends real subscription usage: one Brain triage, one worker run, one verify.
"""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from conftest import SECRET
from fakes import FakeTelegram
from harness.helpers import Site
from test_daemon import ws_down

from swarm import cli
from swarm import daemon as D
from swarm.browser.chrome import ChromeRuntime
from swarm.clock import SystemClock
from swarm.config import Config
from swarm.coordinator.conversation import INTERIM_ACK
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog

pytestmark = pytest.mark.integration

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
PILOT_CHANNEL = "telegram:-100123:42"
HERMES_LIVE = os.environ.get("SWARM_HERMES_LIVE") == "1"
HERMES_URL = os.environ.get("SWARM_HERMES_URL", "http://127.0.0.1:9119")
HERMES_TOKEN = os.environ.get("SWARM_HERMES_TOKEN") or None
NO_HERMES = "kanban card check needs a live Hermes dashboard: set SWARM_HERMES_LIVE=1"
ACK_S = 60
RUN_TIMEOUT_S = 900
FINISHED = ("done", "failed", "cancelled")
# The ack states a time ("받았음 + 기한"): `_fmt_due` ("11/7(토) 18:30"), "쯤까지" for an estimate
ACK_TIME = re.compile(r"\d{1,2}/\d{1,2}\([월화수목금토일]\) \d{2}:\d{2}(쯤)?까지")

PRICING = (
    "<!doctype html><html lang=ko><head><meta charset=utf-8><title>Acme 요금제</title></head>"
    "<body><h1>Acme 요금제</h1><table border=1>"
    "<tr><th>플랜</th><th>월 요금</th><th>포함</th></tr>"
    "<tr><td>Free</td><td>$0</td><td>사용자 1명, 프로젝트 3개</td></tr>"
    "<tr><td>Pro</td><td>$12</td><td>사용자 5명, 프로젝트 무제한</td></tr>"
    "<tr><td>Team</td><td>$49</td><td>사용자 25명, SSO, 감사 로그</td></tr>"
    "</table></body></html>"
).encode()

OUTCOME: dict = {}  # test_pilot_smoke -> test_pilot_kanban_card_done (same module, in order)


@dataclass
class Pilot:
    cfg: Config
    daemon: D.Daemon
    telegram: FakeTelegram

    def db(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.cfg.home / "swarm.db")
        c.row_factory = sqlite3.Row
        return c


@pytest.fixture
def local_site():
    s = Site({"/": PRICING}).start()
    yield s
    s.stop()


@pytest.fixture
async def daemon(tmp_home):
    if not CHROME.exists():
        pytest.skip("Google Chrome not installed")
    if shutil.which("claude") is None:
        pytest.skip("claude CLI not on PATH")
    over = {"chrome_binary": str(CHROME), "claude_model": "haiku"}
    if HERMES_LIVE:
        over["hermes_dashboard_url"] = HERMES_URL
        if HERMES_TOKEN:
            over["hermes_session_token"] = HERMES_TOKEN
    tmp_home.write_config(**over)
    rc = cli.main([
        "--config", str(tmp_home.config), "project", "create",
        "--slug", "compet", "--name", "경쟁사 리서치", "--channel", PILOT_CHANNEL,
        "--objective", "경쟁사 변화를 고객보다 먼저 안다",
        "--kr", "kr_lat|변경 감지 지연(시간)|24|events",
        "--kr", "kr_cov|추적 커버리지(곳)|5|events",
        "--weight", "3", "--profile", "work",
    ])  # fmt: skip
    assert rc == 0
    cfg = D.load_and_validate(tmp_home.config)
    clock = SystemClock()
    chrome_log = SqliteEventLog(  # same swarm.db (WAL), so browser.* events land in the one log
        Database(cfg.home / "swarm.db"), BlobStore(cfg.home / "blobs", clock)
    )
    tg = FakeTelegram()
    started = asyncio.Event()
    box: list[D.Daemon] = []

    def on_started(d: D.Daemon) -> None:
        box.append(d)
        started.set()

    seams = D.Seams(
        browser=ChromeRuntime(cfg.chrome_binary, cfg.profiles, chrome_log, headless=True),
        telegram_transport=tg.transport,
        stop=asyncio.Event(),
        on_started=on_started,
    )
    if not HERMES_LIVE:
        seams.hermes_transport = httpx.MockTransport(lambda r: httpx.Response(503))
        seams.ws_connect = ws_down
    task = asyncio.create_task(D.main(tmp_home.config, seams=seams))
    waiter = asyncio.create_task(started.wait())
    await asyncio.wait({task, waiter}, timeout=30, return_when="FIRST_COMPLETED")
    waiter.cancel()
    try:
        why = task.result() if task.done() else "timeout"
        assert started.is_set(), f"swarmd did not start: {why}"
        yield Pilot(cfg=cfg, daemon=box[0], telegram=tg)
    finally:
        seams.stop.set()
        assert await asyncio.wait_for(task, 60) == D.EXIT_OK


async def mcp_call(pilot: Pilot, tool: str, **args) -> dict:
    return await cli.front_call(pilot.cfg, SECRET, tool, args)


async def wait_for_order(pilot: Pilot, deadline: float) -> str:
    """The order of a tell that answered with the interim ack (Brain slower than 10 s)."""
    while time.monotonic() < deadline:
        with pilot.db() as c:
            r = c.execute(
                "SELECT payload_json FROM events WHERE kind='coordinator.order_queued' "
                "ORDER BY seq DESC LIMIT 1"
            ).fetchone()
        if r is not None:
            return json.loads(r["payload_json"])["order_id"]
        await asyncio.sleep(1)
    raise AssertionError(f"no order queued within {ACK_S} s of the tell")


async def wait_for_late_ack(pilot: Pilot, deadline: float) -> str:
    """After the interim ack the real one is queued to the topic (outbox kind 'ack')."""
    while time.monotonic() < deadline:
        with pilot.db() as c:
            r = c.execute(
                "SELECT text FROM outbox WHERE kind='ack' AND channel=? ORDER BY seq DESC LIMIT 1",
                (PILOT_CHANNEL,),
            ).fetchone()
        if r is not None:
            return r["text"]
        await asyncio.sleep(1)
    raise AssertionError(f"no late ack queued within {ACK_S} s of the interim one")


async def wait_for_run_done(pilot: Pilot, order_id: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        with pilot.db() as c:
            row = c.execute(
                "SELECT id, status FROM runs WHERE order_id=? ORDER BY started_at DESC LIMIT 1",
                (order_id,),
            ).fetchone()
        if row is not None:
            last = (row["id"], row["status"])
            if row["status"] in FINISHED:
                result = pilot.daemon.pool.result(row["id"])
                assert result is not None, f"run {row['id']} finished without a result"
                return dict(result)
        await asyncio.sleep(2)
    raise AssertionError(f"order {order_id}: no finished run within {timeout} s (last {last})")


async def wait_for_card(pilot: Pilot, key: str, timeout: float = 30.0) -> dict | None:
    deadline = time.monotonic() + timeout
    row = None
    while time.monotonic() < deadline:
        with pilot.db() as c:
            row = c.execute("SELECT * FROM kanban_cards WHERE key=?", (key,)).fetchone()
        if row is not None and row["status"] == "done" and not row["dirty"] and row["task_id"]:
            break
        await asyncio.sleep(1)
    return dict(row) if row is not None else None


async def test_pilot_smoke(daemon, local_site):
    # local static "competitor pricing page" -> tell -> ack within 60 s -> Run done (verified,
    # evidence >= 1) -> kanban card done (live Hermes only: test_pilot_kanban_card_done)
    t0 = time.monotonic()
    r = await mcp_call(
        daemon, "swarm_tell", channel=PILOT_CHANNEL,
        text=f"{local_site.url} 가격 페이지 요금 3개를 정리해줘",
    )  # fmt: skip
    assert r["ack"] and time.monotonic() - t0 < ACK_S
    ack = r["ack"] if r["ack"] != INTERIM_ACK else await wait_for_late_ack(daemon, t0 + ACK_S)
    assert ACK_TIME.search(ack), f"the ack states no time: {ack!r}"
    order_id = r["order_id"] or await wait_for_order(daemon, t0 + ACK_S)
    OUTCOME["order_id"] = order_id
    run = await wait_for_run_done(daemon, order_id, timeout=RUN_TIMEOUT_S)
    OUTCOME["run"] = run
    OUTCOME["wall_s"] = round(time.monotonic() - t0)
    print(
        f"\npilot smoke: ack={r['ack']!r} order={order_id} run={run['run_id']} "
        f"status={run['status']} wall={OUTCOME['wall_s']}s summary={run['summary'][:300]!r}"
    )
    assert run["status"] == "done", f"{run['status']}: {run['summary']} {run['failure']}"
    art = run["artifacts"][0]
    assert art["evidence"] and art["status"] == "verified" and art["verifier_run_id"]
    OUTCOME["telegram"] = list(daemon.telegram.sent)  # held outside 08-23: not asserted
    if HERMES_LIVE:
        OUTCOME["card"] = await wait_for_card(daemon, run["run_id"])


@pytest.mark.skipif(not HERMES_LIVE, reason=NO_HERMES)
async def test_pilot_kanban_card_done():
    if "run" not in OUTCOME:
        pytest.skip("needs test_pilot_smoke to have run first in this session (same module)")
    card = OUTCOME.get("card")
    assert card is not None, "no kanban card for the pilot run"
    assert card["status"] == "done" and card["task_id"] and not card["dirty"], card
