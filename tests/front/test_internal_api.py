"""Internal API (worker -> swarmd): asks via pool.raise_ask (idempotent), evidence via
pool.add_evidence, bearer + loopback guard, 127.0.0.1-only bind. The request shapes are the
ones `harness/own/browser_mcp.py` really sends (its serializers are used here)."""

import asyncio
import contextlib
import inspect
import json
from datetime import datetime, timedelta

import httpx
import pytest
from fakes import NotifierSpy

from swarm.clock import SEOUL
from swarm.front.internal_api import (
    IDEMPOTENCY_HEADER,
    InternalApi,
    build_app,
    make_server,
    render_ask,
)
from swarm.harness.own import browser_mcp
from swarm.harness.own.browser_mcp import _ASK_ADAPTER, _EVIDENCE_ADAPTER, BrowserTools
from swarm.inbox.inbox import SqliteInbox
from swarm.projects.store import SqliteProjectStore
from swarm.types import AskDraft, AskTicket, Project, Variant

SECRET = "s3cret-internal"
AUTH = {"Authorization": f"Bearer {SECRET}"}
CH = "telegram:-100123:42"
GENERAL = "telegram:-100123:1"
V = Variant(harness="own", engine="claude", browser="chrome", context_policy="default", model=None)


def make_project() -> Project:
    return Project(
        id="prj_compet", slug="compet", name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": []},
        strategy="", policies=[], autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[], artifacts=[], channel=CH, board="compet", default_profile="work",
        default_variant=V, weight=2, watches=[],
    )  # fmt: skip


def draft(**kw) -> AskDraft:
    d = AskDraft(
        kind="decision", question="어느 안으로 갈까?", options=["A", "B"], recommended="B",
        default_after=timedelta(minutes=30), irreversible=False, evidence=[],
    )  # fmt: skip
    d.update(kw)  # type: ignore[typeddict-item]
    return d


class NoBrain:
    async def decide(self, **kw):  # no policies in these tests: never called
        raise AssertionError("brain called")


class PoolSpy:
    """Stands in for WorkerPool: records calls and, like the real pool, ends in inbox."""

    def __init__(self) -> None:
        self.inbox: SqliteInbox | None = None
        self.asks: list[tuple[str, dict]] = []
        self.evidence: list[tuple[str, dict]] = []
        self.cancels: list[tuple[str, str]] = []
        self.runs = {"run_1": "prj_compet", "run_2": None}
        self.delay = 0.0
        self.ticket: AskTicket | None = None  # canned reply (e.g. closed by policy)

    def run_project(self, run_id):
        return self.runs.get(run_id)

    async def raise_ask(self, run_id, d):
        self.asks.append((run_id, dict(d)))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.ticket is not None:
            return self.ticket
        assert self.inbox is not None
        clean = AskDraft(**{k: v for k, v in d.items() if k != "forced"})
        return await self.inbox.raise_ask(run_id, clean)

    def add_evidence(self, run_id, ev):
        if run_id not in self.runs:
            raise KeyError(run_id)
        self.evidence.append((run_id, dict(ev)))

    async def cancel(self, run_id, *, reason):
        if run_id not in self.runs:
            raise KeyError(run_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        self.cancels.append((run_id, reason))


@pytest.fixture
def stack(db, log, clock, tmp_path):
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    projects.create(make_project())
    pool = PoolSpy()
    inbox = SqliteInbox(
        db, log, projects, NoBrain(), clock,
        run_project=pool.run_project, general_channel=GENERAL,
    )  # fmt: skip
    pool.inbox = inbox
    notifier = NotifierSpy(one_per_ref_id=True)
    api = InternalApi(
        db=db, pool=pool, inbox=inbox, projects=projects, notifier=notifier, clock=clock,
        general_channel=GENERAL, log=log,
    )  # fmt: skip
    return {"pool": pool, "inbox": inbox, "notifier": notifier, "api": api, "projects": projects}


@pytest.fixture
def app(stack):
    return build_app(secret=SECRET, api=stack["api"])


@pytest.fixture
async def client(app):
    t = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=t, base_url="http://127.0.0.1:8765") as c:
        yield c


def ask_body(**kw) -> bytes:
    return _ASK_ADAPTER.dump_json(_ASK_ADAPTER.validate_python(draft(**kw)))


async def post_ask(client, run="run_1", body: bytes | None = None, key: str | None = None):
    headers = {**AUTH, "Content-Type": "application/json"}
    if key:
        headers[IDEMPOTENCY_HEADER] = key
    path = browser_mcp.ASKS_PATH.format(run=run)
    return await client.post(path, content=body or ask_body(), headers=headers)


# --- asks ----------------------------------------------------------------------------


async def test_internal_ask_route_returns_ticket(client, stack):
    r = await post_ask(client)
    assert r.status_code == 200
    data = r.json()
    assert data["ask_id"].startswith("ask_") and data["outcome"] is None and data["number"] == 1
    r2 = await post_ask(client, body=ask_body(question="두 번째?"))
    assert r2.json()["number"] == 2  # topic numbers count up per channel


async def test_ask_route_goes_through_pool_raise_ask(client, stack):
    body = _ASK_ADAPTER.dump_json(
        _ASK_ADAPTER.validate_python(
            draft(kind="approval", options=["승인", "거절"], recommended=None,
                  default_after=None, irreversible=True, forced=["irreversible"])
        )
    )  # fmt: skip
    r = await post_ask(client, body=body)
    assert r.status_code == 200
    [(run_id, d)] = stack["pool"].asks
    assert run_id == "run_1" and d["kind"] == "approval" and d["forced"] == ["irreversible"]
    assert d["default_after"] is None and d["irreversible"] is True


async def test_ask_route_parses_the_mcp_wire_format(client, stack):
    await post_ask(client)
    [(_, d)] = stack["pool"].asks
    assert d["default_after"] == timedelta(minutes=30)  # "PT30M" -> timedelta
    assert d["options"] == ["A", "B"] and d["evidence"] == []


async def test_ask_route_is_idempotent(client, stack):
    a = (await post_ask(client, key="k1")).json()
    b = (await post_ask(client, key="k1")).json()
    assert a == b and len(stack["pool"].asks) == 1
    assert [m["ref_id"] for m in stack["notifier"].sent] == [a["ask_id"]]  # ref dedupe
    c = (await post_ask(client, key="k2")).json()
    assert c["ask_id"] != a["ask_id"]
    assert len(stack["inbox"].open_asks()) == 2


async def test_idempotent_replay_only_while_the_ask_is_open(client, stack):
    a = (await post_ask(client, key="k1")).json()
    await stack["inbox"].answer(a["ask_id"], choice="A", raw_text="A", via=CH)
    b = (await post_ask(client, key="k1")).json()  # same question asked again later
    assert b["ask_id"] != a["ask_id"] and len(stack["pool"].asks) == 2


async def test_idempotency_survives_restart(client, stack, db, clock):
    a = (await post_ask(client, key="k1")).json()
    api2 = InternalApi(
        db=db, pool=stack["pool"], inbox=stack["inbox"], projects=stack["projects"],
        notifier=NotifierSpy(one_per_ref_id=True),
        clock=clock, general_channel=GENERAL,
    )  # fmt: skip
    t = httpx.ASGITransport(app=build_app(secret=SECRET, api=api2))
    async with httpx.AsyncClient(transport=t, base_url="http://127.0.0.1:8765") as c2:
        b = (await post_ask(c2, key="k1")).json()
    assert b == a and len(stack["pool"].asks) == 1


async def test_concurrent_retries_raise_one_ask(client, stack):
    stack["pool"].delay = 0.05
    rs = await asyncio.gather(*(post_ask(client, key="same") for _ in range(3)))
    assert len({r.json()["ask_id"] for r in rs}) == 1 and len(stack["pool"].asks) == 1


async def test_ask_is_announced_to_the_project_topic(client, stack):
    data = (await post_ask(client)).json()
    [m] = stack["notifier"].sent
    assert m["kind"] == "ask" and m["ref_id"] == data["ask_id"] and m["channel"] == CH
    assert m["urgent"] is False  # reversible with a default: waits for the alert window
    first = m["text"].splitlines()[0]
    assert first.startswith("🙋 결정 · #1") and "어느 안으로 갈까?" in first
    assert "B" in m["text"] and "30분" in m["text"]


async def test_blocked_ask_is_urgent_and_unmapped_run_goes_to_general(client, stack):
    body = ask_body(kind="approval", options=["승인", "거절"], recommended=None,
                    default_after=None, irreversible=True)  # fmt: skip
    await post_ask(client, run="run_2", body=body)
    [m] = stack["notifier"].sent
    assert m["urgent"] is True and m["channel"] == GENERAL
    assert m["text"].startswith("🔐 승인 · #1")


async def test_policy_closed_ask_is_not_announced(client, stack):
    stack["pool"].ticket = AskTicket(
        ask_id="ask_x",
        outcome={"ask_id": "ask_x", "run_id": "run_1", "status": "answered", "choice": "A",
                 "raw_text": None, "via": "policy"},
    )  # fmt: skip
    stack["pool"].inbox.topic_number = lambda a: 3  # type: ignore[method-assign]
    data = (await post_ask(client)).json()
    assert data["outcome"]["choice"] == "A" and data["number"] == 3
    assert stack["notifier"].sent == []


async def test_ask_route_rejects_bad_body(client, stack):
    r = await post_ask(client, body=b'{"kind": "nope"}')
    assert r.status_code == 422 and stack["pool"].asks == []
    r = await post_ask(client, body=b"not json")
    assert r.status_code == 422


async def test_ask_route_reports_pool_failure(client, stack):
    async def boom(run_id, d):
        raise RuntimeError("db locked")

    stack["pool"].raise_ask = boom
    r = await post_ask(client)
    assert r.status_code == 500 and "error" in r.json()


def test_render_ask_kinds():
    view = {"kind": "takeover", "question": "B사 로그인 필요", "options": [],
            "recommended": None, "default_after": None, "irreversible": True}  # fmt: skip
    text = render_ask(4, view)  # type: ignore[arg-type]
    assert text.startswith("🖐 직접 처리 · #4 B사 로그인 필요") and len(text) <= 3500
    long = render_ask(1, {**view, "kind": "decision", "question": "질문" * 3000})  # type: ignore[arg-type]
    assert len(long) <= 3500


# --- evidence ------------------------------------------------------------------------


async def test_evidence_route_maps_the_mcp_body(client, stack):
    at = datetime(2026, 11, 5, 10, 0, tzinfo=SEOUL)
    ev = {"url": "https://c.example/pricing", "screenshot": "/blobs/s1.png",
          "captured_at": at, "text": "무료 티어 신설"}  # fmt: skip
    r = await client.post(
        browser_mcp.EVIDENCE_PATH.format(run="run_1"),
        content=_EVIDENCE_ADAPTER.dump_json(ev), headers=AUTH,
    )  # fmt: skip
    eid = r.json()["evidence_id"]
    assert r.status_code == 200 and eid.startswith("ev_")
    [(run_id, got)] = stack["pool"].evidence
    assert run_id == "run_1" and got == {**ev, "id": eid}  # the id is stored with the item


async def test_evidence_route_accepts_claim_shape(client, stack):
    body = {"url": "https://c.example", "claim": "가격 인상", "screenshot_path": None,
            "ts": "2026-11-05T10:00:00+09:00"}  # fmt: skip
    r = await client.post("/internal/runs/run_1/evidence", json=body, headers=AUTH)
    assert r.status_code == 200
    [(_, got)] = stack["pool"].evidence
    assert got == {"url": "https://c.example", "text": "가격 인상", "screenshot": None,
                   "captured_at": datetime(2026, 11, 5, 10, 0, tzinfo=SEOUL),
                   "id": r.json()["evidence_id"]}  # fmt: skip


async def test_evidence_naive_time_is_seoul(client, stack):
    body = {"url": None, "text": "x", "screenshot": None, "captured_at": "2026-11-05T10:00:00"}
    await client.post("/internal/runs/run_1/evidence", json=body, headers=AUTH)
    assert stack["pool"].evidence[0][1]["captured_at"].tzinfo is not None


async def test_evidence_unknown_run_is_404(client, stack):
    body = {"url": None, "text": "x", "screenshot": None, "captured_at": "2026-11-05T10:00:00Z"}
    r = await client.post("/internal/runs/run_nope/evidence", json=body, headers=AUTH)
    assert r.status_code == 404


async def test_evidence_bad_body_is_422(client, stack):
    r = await client.post("/internal/runs/run_1/evidence", json={"url": 3}, headers=AUTH)
    assert r.status_code == 422 and stack["pool"].evidence == []


# --- guard ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer wrong"},
        {"Authorization": SECRET},
        {"Authorization": "Basic x"},
    ],
)
async def test_bearer_required_on_internal_routes(client, stack, headers):
    for path in (
        "/internal/runs/run_1/asks",
        "/internal/runs/run_1/evidence",
        "/internal/runs/run_1/cancel",
    ):
        r = await client.post(path, content=ask_body(), headers=headers)
        assert r.status_code == 401, path
        assert r.headers["www-authenticate"].startswith("Bearer")
    assert stack["pool"].asks == [] and stack["pool"].evidence == []
    assert stack["pool"].cancels == []


async def test_bearer_rejection_is_logged_without_the_token(client, caplog):
    caplog.set_level("WARNING", logger="swarm.front.internal_api")
    bad = "Bearer wrong-token-xyz"
    r = await client.post("/internal/runs/run_1/asks", content=ask_body(),
                          headers={"Authorization": bad})  # fmt: skip
    assert r.status_code == 401
    assert "bearer rejected (401) POST /internal/runs/run_1/asks from 127.0.0.1" in caplog.text
    assert "wrong-token-xyz" not in caplog.text and SECRET not in caplog.text
    caplog.clear()
    await client.post("/internal/runs/run_1/asks", content=ask_body(), headers=AUTH)
    assert "bearer rejected" not in caplog.text


async def test_health_is_open_on_loopback(client):
    r = await client.get("/internal/health")
    assert r.status_code == 200 and r.json() == {"ok": True}


async def test_non_loopback_client_is_refused(app):
    t = httpx.ASGITransport(app=app, client=("10.0.0.5", 5000))
    async with httpx.AsyncClient(transport=t, base_url="http://127.0.0.1:8765") as c:
        r = await c.get("/internal/health")
        assert r.status_code == 403
        r = await c.post("/internal/runs/run_1/asks", content=ask_body(), headers=AUTH)
        assert r.status_code == 403


def test_build_app_refuses_empty_secret(stack):
    with pytest.raises(ValueError):
        build_app(secret="", api=stack["api"])


@contextlib.asynccontextmanager
async def serving(app):
    server = make_server(app, port=0)
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started
    try:
        yield server
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)


async def test_internal_binds_loopback_only(app):
    assert "host" not in inspect.signature(make_server).parameters  # no way to widen it
    async with serving(app) as server:
        names = [s.getsockname() for srv in server.servers for s in srv.sockets]
        assert names and all(n[0] == "127.0.0.1" for n in names)


# --- the real client: browser_mcp against this API -----------------------------------


async def test_browser_mcp_client_round_trip(app, stack, tmp_path):
    async with serving(app) as server:
        port = server.servers[0].sockets[0].getsockname()[1]
        tools = BrowserTools(
            cdp_url="ws://unused", target_id="T1", run_id="run_1",
            internal=f"http://127.0.0.1:{port}", token=SECRET, blob_dir=tmp_path,
            policy="compressed",
        )  # fmt: skip
        reply = await tools.ask_human("decision", "A?", ["A", "B"], "B", 30, False)
        assert reply.startswith(browser_mcp.ASK_SENT_PREFIX) and "(#1)" in reply
        again = await tools.ask_human("decision", "A?", ["A", "B"], "B", 30, False)
        assert again == reply and len(stack["pool"].asks) == 1  # retry -> same Ask
        ev = await tools.record_evidence("https://c.example", "무료 티어", False)
        assert "증거를 기록했습니다(ev_" in ev
        assert stack["pool"].evidence[0][1]["text"] == "무료 티어"
        bad = BrowserTools(
            cdp_url="ws://unused", target_id="T1", run_id="run_1",
            internal=f"http://127.0.0.1:{port}", token="wrong", blob_dir=tmp_path,
            policy="compressed",
        )  # fmt: skip
        assert "HTTP 401" in await bad.ask_human("decision", "B?", ["A"], None, None, False)


def test_routes_match_the_mcp_client_paths(stack):
    paths = {r.path for r in stack["api"].routes()}
    assert browser_mcp.ASKS_PATH.replace("{run}", "{run_id}") in paths
    assert browser_mcp.EVIDENCE_PATH.replace("{run}", "{run_id}") in paths
    assert json.dumps(sorted(paths))  # sanity


# --- an Ask must always reach a human ----------------------------------------------------


async def test_announce_falls_back_to_general_when_the_project_is_gone(client, stack):
    class Gone:  # the project was removed between raise and announce
        def get(self, pid):
            raise KeyError(pid)

    stack["api"]._projects = Gone()
    await post_ask(client)
    [m] = stack["notifier"].sent
    assert m["channel"] == GENERAL and m["kind"] == "ask"


async def test_announce_falls_back_to_general_when_the_channel_is_rejected(client, stack):
    stack["notifier"].reject.add(CH)
    await post_ask(client)
    [m] = stack["notifier"].sent
    assert m["channel"] == GENERAL


async def test_announce_failure_is_healed_by_the_sweep(client, stack, clock):
    stack["notifier"].fail_next = 1
    data = (await post_ask(client)).json()
    assert data["number"] == 1 and stack["notifier"].sent == []  # the worker still parks
    api = stack["api"]
    assert await api.sweep() == 0  # first sight: give a just-raised Ask its grace period
    clock.advance(timedelta(seconds=61))
    assert await api.sweep() == 1
    [m] = stack["notifier"].sent
    assert m["ref_id"] == data["ask_id"] and m["text"].startswith("🙋 결정 · #1")
    assert await api.sweep() == 0  # announced once


async def test_crash_between_raise_and_enqueue_is_healed_at_startup(stack):
    t = await stack["pool"].raise_ask("run_1", draft())  # raised, process died before announce
    assert stack["notifier"].sent == []
    stop = asyncio.Event()
    task = asyncio.create_task(stack["api"].run_sweeps(stop))
    for _ in range(100):
        if stack["notifier"].sent:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert [m["ref_id"] for m in stack["notifier"].sent] == [t["ask_id"]]


async def test_sweep_never_double_announces(db, stack, clock, fake_tg):
    from swarm.front.notifier import TelegramNotifier

    from .conftest import TOKEN

    tg = TelegramNotifier(TOKEN, db, stack["inbox"], clock, transport=fake_tg.transport)
    api = InternalApi(
        db=db, pool=stack["pool"], inbox=stack["inbox"], projects=stack["projects"],
        notifier=tg, clock=clock, general_channel=GENERAL,
    )  # fmt: skip
    # the Coordinator's own confirmation Ask: raised and enqueued by the Coordinator
    confirm = await stack["inbox"].raise_ask("run_1", draft(question="목표를 바꿀까요?"))
    await tg.send({"channel": CH, "kind": "ask", "text": "🙋 결정 · #1 목표를 바꿀까요? (코디)",
                   "ref_id": confirm["ask_id"], "urgent": False})  # fmt: skip
    # a worker Ask announced by the route
    t = httpx.ASGITransport(app=build_app(secret=SECRET, api=api))
    async with httpx.AsyncClient(transport=t, base_url="http://127.0.0.1:8765") as c:
        worker = (await post_ask(c, body=ask_body(question="워커 질문?"))).json()
    assert await api.sweep(grace=timedelta(0)) == 0
    await tg.drain_once()
    assert await api.sweep(grace=timedelta(0)) == 0  # sent: still nothing to do
    texts = fake_tg.texts()
    assert len(texts) == 2 and texts[0].endswith("(코디)") and "워커 질문?" in texts[1]
    assert {a["ask_id"] for a in stack["inbox"].open_asks()} == {confirm["ask_id"],
                                                               worker["ask_id"]}  # fmt: skip
    await tg.aclose()


async def test_sweep_prunes_keys_of_closed_asks(client, stack, db):
    a = (await post_ask(client, key="k1")).json()
    await post_ask(client, key="k2", body=ask_body(question="둘?"))
    await stack["inbox"].answer(a["ask_id"], choice="A", raw_text="A", via=CH)
    await stack["api"].sweep()
    keys = [r["key"] for r in db.execute("SELECT key FROM front_ask_keys").fetchall()]
    assert keys == ["k2"]


def test_urgency_follows_the_blocked_wording():
    from swarm.front.internal_api import is_blocked

    base = {"kind": "decision", "question": "Q?", "options": ["A", "B"], "irreversible": False}
    waits = {**base, "recommended": "B", "default_after": timedelta(minutes=30)}
    no_rec = {**base, "recommended": None, "default_after": timedelta(minutes=30)}
    no_default = {**base, "recommended": "B", "default_after": None}
    assert not is_blocked(waits) and "진행해요" in render_ask(1, waits)  # type: ignore[arg-type]
    for v in (no_rec, no_default):
        assert is_blocked(v) and "멈춰 있어요" in render_ask(1, v)  # type: ignore[arg-type]


# --- guard edge cases ----------------------------------------------------------------


async def asgi_call(app, scope: dict) -> list[dict]:
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    base = {"http_version": "1.1", "scheme": "http", "query_string": b"", "root_path": "",
            "server": ("127.0.0.1", 8765), "headers": [(b"host", b"127.0.0.1:8765")]}  # fmt: skip
    await app({**base, **scope}, receive, send)
    return sent


async def test_guard_refuses_a_scope_without_client(app):
    sent = await asgi_call(app, {"type": "http", "method": "GET", "path": "/internal/health",
                                 "raw_path": b"/internal/health"})  # fmt: skip
    assert sent[0]["status"] == 403


async def test_guard_closes_unauthenticated_websockets(app):
    sent = await asgi_call(app, {"type": "websocket", "path": "/mcp", "raw_path": b"/mcp",
                                 "client": ("127.0.0.1", 5), "subprotocols": []})  # fmt: skip
    assert sent == [{"type": "websocket.close", "code": 1008}]


async def test_health_trailing_slash_is_not_an_open_door(client, stack):
    r = await client.get("/internal/health/")
    assert r.status_code == 401
    r = await client.post("/internal/health/../runs/run_1/asks", content=ask_body())
    assert r.status_code == 401 and stack["pool"].asks == []


# --- cancel (watchdog: stuck runs) -----------------------------------------------------


async def test_cancel_route_calls_pool_cancel_with_the_reason(client, stack):
    r = await client.post("/internal/runs/run_1/cancel", json={"reason": "stuck"}, headers=AUTH)
    assert r.status_code == 200 and r.json() == {"ok": True, "run_id": "run_1"}
    assert stack["pool"].cancels == [("run_1", "stuck")]


async def test_cancel_route_unknown_run_is_404_and_bad_body_422(client, stack):
    r = await client.post("/internal/runs/run_x/cancel", json={"reason": "stuck"}, headers=AUTH)
    assert r.status_code == 404
    for body in ({}, {"reason": ""}, {"reason": 3}, []):
        r = await client.post("/internal/runs/run_1/cancel", json=body, headers=AUTH)
        assert r.status_code == 422, body
    assert stack["pool"].cancels == []


async def test_slow_cancel_returns_202_and_still_finishes(client, stack, monkeypatch):
    from swarm.front import internal_api

    monkeypatch.setattr(internal_api, "CANCEL_WAIT_S", 0.01)
    stack["pool"].delay = 0.1
    r = await client.post("/internal/runs/run_1/cancel", json={"reason": "stuck"}, headers=AUTH)
    assert r.status_code == 202 and r.json()["ok"] is True
    await asyncio.sleep(0.2)
    assert stack["pool"].cancels == [("run_1", "stuck")]


async def test_server_leaves_signal_handlers_alone(app):
    """swarmd owns SIGTERM (ordered shutdown); uvicorn must not replace or re-raise it."""
    import signal

    before = signal.getsignal(signal.SIGTERM)
    server = make_server(app, port=0)
    task = asyncio.create_task(server.serve())
    for _ in range(300):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started and signal.getsignal(signal.SIGTERM) is before
    server.should_exit = True
    await asyncio.wait_for(task, 5)
    assert signal.getsignal(signal.SIGTERM) is before
