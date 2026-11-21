"""WorkerPool on the real stack: headless Chrome + armed ActionGuard + real `claude` (haiku,
subscription) through the own harness and the real Verifier, against a local static page. The
worker must call record_evidence; a tiny stand-in for the internal API hands the evidence
to `pool.add_evidence`. One cheap task."""

import asyncio
import shutil
import socket
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from swarm.clock import SystemClock
from swarm.engine.claude import ClaudeEngine
from swarm.harness.own.adapter import OwnHarness
from swarm.harness.own.context import RolloverBrief
from swarm.harness.registry import COMPATIBLE
from swarm.projects.store import SqliteProjectStore
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import Evidence, QuotaLease, Variant
from swarm.workers.pool import WorkerPool
from swarm.workers.verify import Verifier

from .conftest import FakeInbox, make_project, order

pytestmark = pytest.mark.integration

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
V = Variant(harness="own", engine="claude", browser="chrome", context_policy="", model="haiku")


PAGE = b"<!doctype html><title>Two</title><h1>Second page</h1>"


class Site:
    """Local static page (http://localhost:<port>/two)."""

    def __init__(self) -> None:
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a) -> None:
                pass

            def do_GET(self) -> None:
                ok = self.path.split("?", 1)[0] == "/two"
                body = PAGE if ok else b"not found"
                self.send_response(200 if ok else 404)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://localhost:{self.httpd.server_port}"
        self._t = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> "Site":
        self._t.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class Ledger:
    def __init__(self) -> None:
        self.leases: list[QuotaLease] = []
        self.released: list[QuotaLease] = []

    def lease(self, *, engine_pref, purpose, max_minutes=60):
        lease = QuotaLease(
            id=f"lse_{len(self.leases) + 1}",
            engine="claude",
            purpose=purpose,
            expires_at=datetime.now().astimezone() + timedelta(minutes=max_minutes + 10),
        )
        self.leases.append(lease)
        return lease

    def release(self, lease) -> None:
        self.released.append(lease)

    def observe(self, *, engine, signal) -> None:
        pass


class Registry:
    def __init__(self, harness) -> None:
        self.harness = harness

    def get(self, harness_id, *, context_policy=None):
        return self.harness


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def site():
    s = Site().start()
    yield s
    s.stop()


@pytest.fixture
async def chrome(tmp_path):
    from swarm.browser.chrome import ChromeRuntime

    if not CHROME.exists():
        pytest.skip("Google Chrome not installed")
    if shutil.which("claude") is None:
        pytest.skip("claude CLI not installed")
    clock = SystemClock()
    log = SqliteEventLog(Database(tmp_path / "chrome.db"), BlobStore(tmp_path / "cblobs", clock))
    dirs = {"test": Path(tempfile.mkdtemp(prefix="swarm-pool-"))}
    rt = ChromeRuntime(CHROME, dirs, log, headless=True, reconnect_delays=(0.1, 0.1, 0.1))
    yield rt
    await rt.shutdown()
    shutil.rmtree(dirs["test"], ignore_errors=True)


async def test_pool_real_stack_done_with_verified_evidence(chrome, site, tmp_path):
    clock = SystemClock()
    db = Database(tmp_path / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    projects.create(make_project())
    ledger = Ledger()
    engine = ClaudeEngine(binary="claude", model="haiku", workdir=tmp_path)
    guards: dict = {}

    async def guard_for(browser_id, profile):
        if profile not in guards:
            guards[profile] = await chrome.open_guard(profile, allowlist=["localhost"])
        return guards[profile]

    pool_box: list[WorkerPool] = []

    async def evidence(request: Request) -> JSONResponse:
        body = await request.json()
        ev = Evidence(
            url=body.get("url"),
            screenshot=body.get("screenshot"),
            captured_at=datetime.fromisoformat(body["captured_at"]),
            text=body.get("text"),
        )
        pool_box[0].add_evidence(request.path_params["run"], ev)
        return JSONResponse(
            {"evidence_id": f"ev_{len(pool_box[0].evidence(request.path_params['run']))}"}
        )

    port = free_port()
    app = Starlette(routes=[Route("/internal/runs/{run}/evidence", evidence, methods=["POST"])])
    server = uvicorn.Server(uvicorn.Config(app, port=port, log_level="error"))
    serving = asyncio.create_task(server.serve())
    token = tmp_path / "token"
    token.write_text("tok-123\n")
    harness = OwnHarness(
        RolloverBrief(),
        mcp_entry=str(Path(sys.executable).parent / "swarm-browser-mcp"),
        internal_url=f"http://127.0.0.1:{port}",
        token_file=token,
    )
    pool = WorkerPool(
        db=db,
        projects=projects,
        inbox=FakeInbox(),
        ledger=ledger,
        engines={"claude": engine},
        compatible=COMPATIBLE,
        browsers={"chrome": chrome},
        guards=guard_for,
        harnesses=Registry(harness),
        log=log,
        clock=clock,
        output_root=tmp_path / "runs",
        verifier=Verifier(engine, ledger),
    )
    pool_box.append(pool)
    url = site.url + "/two"
    o = order(
        profile="test",
        intent=(
            f"{url} 을 열고 페이지 제목(title)을 확인하라. 확인한 뒤 record_evidence 도구로"
            f" url={url}, claim='페이지 제목은 Two', screenshot=true 증거를 남기고, 제목을"
            " 한 줄로 보고하라. 다른 일은 하지 마라."
        ),
        done_criteria=["페이지 제목이 보고되었고 그 페이지의 증거가 record_evidence로 남아 있다"],
        budget={"max_minutes": 5},
    )
    try:
        while not server.started:
            await asyncio.sleep(0.05)
        r = await pool.run(o, V, lease=ledger.lease(engine_pref=["claude"], purpose="advance"))
    finally:
        server.should_exit = True
        await serving
        for g in guards.values():
            await g.close()
    evs = pool.evidence(r["run_id"])
    assert r["status"] == "done", (r["summary"], r["failure"], evs)
    assert evs and evs[0]["url"].startswith(site.url)
    assert r["artifacts"] and all(
        a["status"] == "verified"
        and a["evidence"]
        and a["verifier_run_id"]
        and a["verifier_same_engine"] is True
        for a in r["artifacts"]
    )
    assert "Two" in r["summary"]
    assert [x["purpose"] for x in ledger.leases] == ["advance"]  # verify used the run's lease
    assert len(ledger.released) == len(ledger.leases)
