"""SwarmFront MCP: five tools, streamable HTTP at /mcp behind the bearer, every
tool back within 10 s, status <= 3,500 chars, Hermes' text passed through verbatim."""

import asyncio
import contextlib
import time
from datetime import timedelta

import httpx
import httpx2
import pytest
from fakes import NotifierSpy
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from swarm.front.internal_api import InternalApi, build_app, make_server
from swarm.front.mcp_server import (
    REPORT_LATER,
    TOOL_NAMES,
    SwarmFrontService,
    build_mcp,
    streamable_app,
)
from swarm.inbox.inbox import SqliteInbox
from swarm.projects.store import SqliteProjectStore

from .test_internal_api import CH, GENERAL, SECRET, NoBrain, PoolSpy, draft
from .test_internal_api import make_project as project

AUTH = {"Authorization": f"Bearer {SECRET}"}


class FakeCoordinator:
    def __init__(self) -> None:
        self.tells: list[tuple[str, str]] = []
        self.status_text = "📋 경쟁사 · 순항"
        self.report_delay = 0.0
        self.report_gate: asyncio.Event | None = None  # set -> report() waits for it
        self.status_delay = 0.0
        self.reports: list[tuple[str, str]] = []

    async def handle_tell(self, channel, text):
        self.tells.append((channel, text))
        return {"ack": "받았어요.", "project_id": "prj_compet", "order_id": "wo_1",
                "closed_ask_id": None}  # fmt: skip

    async def status(self, channel):
        await asyncio.sleep(self.status_delay)
        return self.status_text

    async def report(self, channel, kind):
        self.reports.append((channel, kind))
        if self.report_gate is not None:
            await self.report_gate.wait()
        await asyncio.sleep(self.report_delay)
        return f"📊 {kind} 보고 · 순항"


@pytest.fixture
def parts(db, log, clock, tmp_path):
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    projects.create(project())
    pool = PoolSpy()
    inbox = SqliteInbox(
        db, log, projects, NoBrain(), clock,
        run_project=pool.run_project, general_channel=GENERAL,
    )  # fmt: skip
    pool.inbox = inbox
    notifier = NotifierSpy(one_per_ref_id=True)
    coordinator = FakeCoordinator()
    front = SwarmFrontService(
        coordinator=coordinator, inbox=inbox, projects=projects, notifier=notifier, wait_s=0.3
    )
    api = InternalApi(
        db=db, pool=pool, inbox=inbox, projects=projects, notifier=notifier, clock=clock,
        general_channel=GENERAL,
    )  # fmt: skip
    return {"front": front, "coordinator": coordinator, "inbox": inbox, "pool": pool,
            "notifier": notifier, "api": api, "projects": projects}  # fmt: skip


@contextlib.asynccontextmanager
async def connect(parts):
    """In-process MCP client (an anyio scope: entered and left in the test's own task)."""
    try:
        async with Client(build_mcp(parts["front"])) as c:
            yield c
    finally:
        await parts["front"].aclose()


def text_of(res) -> str:
    return "\n".join(c.text for c in res.content if getattr(c, "type", "") == "text")


async def test_mcp_tools_listed(parts):
    async with connect(parts) as mcp_test_client:
        assert {t.name for t in (await mcp_test_client.list_tools()).tools} == {
            "swarm_tell", "swarm_status", "swarm_inbox", "swarm_projects", "swarm_report",
        }  # fmt: skip
        assert set(TOOL_NAMES) == {t.name for t in (await mcp_test_client.list_tools()).tools}


async def test_tell_passes_the_text_verbatim(parts):
    async with connect(parts) as mcp_test_client:
        raw = "  #2 B로 가 *굵게* _밑줄_ \n둘째 줄 "
        res = await mcp_test_client.call_tool("swarm_tell", {"channel": CH, "text": raw})
        assert not res.is_error
        assert parts["coordinator"].tells == [(CH, raw)]
        assert res.structured_content["ack"] == "받았어요." and res.structured_content["order_id"]


async def test_status_under_3500_chars(parts):
    async with connect(parts) as mcp_test_client:
        parts["coordinator"].status_text = "가" * 9000
        res = await mcp_test_client.call_tool("swarm_status", {"channel": CH})
        assert not res.is_error and 0 < len(res.structured_content["result"]) <= 3500


async def test_slow_status_still_returns_in_time(parts):
    async with connect(parts) as mcp_test_client:
        parts["coordinator"].status_delay = 5
        t0 = time.monotonic()
        res = await mcp_test_client.call_tool("swarm_status", {"channel": CH})
        assert time.monotonic() - t0 < 2 and not res.is_error
        assert res.structured_content["result"].startswith("⚠️")


async def test_report_returns_text(parts):
    async with connect(parts) as mcp_test_client:
        res = await mcp_test_client.call_tool("swarm_report", {"channel": CH, "kind": "weekly"})
        assert res.structured_content["result"] == "📊 weekly 보고 · 순항"
        assert parts["coordinator"].reports == [(CH, "weekly")]


async def test_report_rejects_unknown_kind(parts):
    async with connect(parts) as mcp_test_client:
        res = await mcp_test_client.call_tool("swarm_report", {"channel": CH, "kind": "monthly"})
        assert res.is_error and parts["coordinator"].reports == []


async def test_slow_report_comes_later_through_the_notifier(parts):
    async with connect(parts) as mcp_test_client:
        gate = parts["coordinator"].report_gate = asyncio.Event()  # never ready in time
        t0 = time.monotonic()
        res = await mcp_test_client.call_tool("swarm_report", {"channel": CH, "kind": "daily"})
        assert res.structured_content["result"] == REPORT_LATER  # did not wait for the report
        assert time.monotonic() - t0 < 5  # the tell ack budget is 10 s; front wait_s is 0.3
        gate.set()
        for _ in range(100):
            if parts["notifier"].sent:
                break
            await asyncio.sleep(0.02)
        [m] = parts["notifier"].sent
        assert m["kind"] == "report" and m["channel"] == CH and m["text"] == "📊 daily 보고 · 순항"


async def test_inbox_lists_open_asks_with_numbers(parts):
    async with connect(parts) as mcp_test_client:
        t = await parts["inbox"].raise_ask("run_1", draft(default_after=timedelta(minutes=30)))
        res = await mcp_test_client.call_tool("swarm_inbox", {})
        [item] = res.structured_content["result"]
        assert item["ask_id"] == t["ask_id"] and item["number"] == 1
        assert item["question"] == "어느 안으로 갈까?" and item["deadline"] is None  # not sent yet
        assert item["project_id"] == "prj_compet"


async def test_projects_lists_views(parts):
    async with connect(parts) as mcp_test_client:
        await parts["inbox"].raise_ask("run_1", draft())
        res = await mcp_test_client.call_tool("swarm_projects", {})
        assert res.structured_content["result"] == [
            {"id": "prj_compet", "slug": "compet", "name": "경쟁사", "channel": CH,
             "objective": "경쟁사 변화를 먼저 안다", "open_asks": 1}
        ]  # fmt: skip


async def test_tell_failure_still_answers(parts):
    async with connect(parts) as mcp_test_client:

        async def boom(channel, text):
            raise RuntimeError("bug")

        parts["coordinator"].handle_tell = boom
        res = await mcp_test_client.call_tool("swarm_tell", {"channel": CH, "text": "hi"})
        assert not res.is_error and res.structured_content["ack"].startswith("⚠️")


# --- over HTTP: one app, bearer, loopback --------------------------------------------


@contextlib.asynccontextmanager
async def served(parts):
    app = build_app(
        secret=SECRET, api=parts["api"], mcp_app=streamable_app(build_mcp(parts["front"]))
    )
    server = make_server(app, port=0)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)
        await parts["front"].aclose()


INIT = {
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-06-18", "capabilities": {},
               "clientInfo": {"name": "t", "version": "0"}},
}  # fmt: skip
MCP_HEADERS = {"Accept": "application/json, text/event-stream"}


async def test_mcp_requires_bearer(parts):
    async with served(parts) as base, httpx.AsyncClient(base_url=base) as c:
        r = await c.post("/mcp", json=INIT, headers=MCP_HEADERS)
        assert r.status_code == 401
        r = await c.post("/mcp", json=INIT, headers={**MCP_HEADERS, "Authorization": "Bearer nope"})
        assert r.status_code == 401
        r = await c.post("/mcp", json=INIT, headers={**MCP_HEADERS, **AUTH})
        assert r.status_code == 200


async def test_mcp_over_streamable_http_with_bearer(parts):
    async with served(parts) as base:
        http = httpx2.AsyncClient(headers=AUTH, timeout=10)
        async with http, Client(streamable_http_client(f"{base}/mcp", http_client=http)) as c:
            names = {t.name for t in (await c.list_tools()).tools}
            assert names == set(TOOL_NAMES)
            res = await c.call_tool("swarm_tell", {"channel": CH, "text": "C사 봐줘"})
            assert res.structured_content["ack"] == "받았어요."
        assert parts["coordinator"].tells == [(CH, "C사 봐줘")]


async def test_internal_routes_share_the_mcp_app(parts):
    async with served(parts) as base, httpx.AsyncClient(base_url=base) as c:
        assert (await c.get("/internal/health")).json() == {"ok": True}
        body = {"kind": "decision", "question": "Q?", "options": ["A"], "recommended": None,
                "default_after": None, "irreversible": False, "evidence": []}  # fmt: skip
        r = await c.post("/internal/runs/run_1/asks", json=body, headers=AUTH)
        assert r.status_code == 200 and r.json()["number"] == 1
        r = await c.post("/internal/runs/run_1/asks", json=body)
        assert r.status_code == 401


async def test_cancelled_report_call_still_delivers_the_report(parts):
    parts["coordinator"].report_delay = 0.2
    call = asyncio.create_task(parts["front"].swarm_report(channel=CH, kind="daily"))
    await asyncio.sleep(0.05)
    call.cancel()  # Hermes gave up / the MCP request was cancelled before our timeout
    with pytest.raises(asyncio.CancelledError):
        await call
    for _ in range(100):
        if parts["notifier"].sent:
            break
        await asyncio.sleep(0.01)
    [m] = parts["notifier"].sent
    assert m["kind"] == "report" and m["text"] == "📊 daily 보고 · 순항"
    await parts["front"].aclose()
