"""KanbanMirror: mirror of the backlog + runs onto the Hermes
dashboard kanban. ProjectStore is canonical; the board is a mirror. A fake Hermes (REST via
httpx.MockTransport + an in-process WS emitter) stands in for the dashboard: no network."""

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from swarm.front.kanban import HermesKanbanMirror
from swarm.front.kanban.cards import ARCHIVE_AFTER, card_from_backlog, card_from_run, change_event
from swarm.front.kanban.hermes import TOKEN_HEADER
from swarm.front.kanban.mirror import REVERT_WINDOW
from swarm.ids import new_id
from swarm.projects.store import SqliteProjectStore
from swarm.types import BacklogItem, CardView, Project, WorkerRun, WorkOrder

BASE = "http://127.0.0.1:9119"
SECRET = "hermes-SESSION-secret-7f3a"
PID = "prj_compet"
BOARD = "compet"
PLUGIN = "/api/plugins/kanban"


# --- fake Hermes ----------------------------------------------------------------------


class FakeWs:
    def __init__(self, url: str, headers: dict) -> None:
        self.url = url
        self.headers = headers
        self.queue: asyncio.Queue = asyncio.Queue()
        self.idle = asyncio.Event()
        self.closed = False

    async def recv(self) -> str:
        if self.queue.empty():
            self.idle.set()
        msg = await self.queue.get()
        self.idle.clear()
        if isinstance(msg, Exception):
            raise msg
        return msg

    async def close(self) -> None:
        self.closed = True


class FakeHermes:
    """`tasks` in creation order; `down=True` refuses every REST call and WS connect."""

    def __init__(self) -> None:
        self.tasks: list[dict] = []
        self.comments: list[dict] = []
        self.requests: list[httpx.Request] = []
        self.down = False
        self.fail_next: list = []  # scripted: int status or Exception, consumed per request
        self.always: int | None = None  # every request answers this status (e.g. 401)
        self.gate: asyncio.Event | None = None  # board GET snapshots, then waits for it
        self.gate_waiting = asyncio.Event()
        self.frozen_time = False  # updated_at stops moving (coarse Hermes timestamps)
        self.ws_log: list[dict] = []
        self.conns: list[FakeWs] = []
        self._clock = 0
        self._ids = 0
        self.transport = httpx.MockTransport(self._handle)

    # REST
    def _tick(self) -> str:
        if not self.frozen_time:
            self._clock += 1
        return f"2026-11-05T10:00:{self._clock:02d}+09:00"

    def by_id(self, task_id: str) -> dict | None:
        return next((t for t in self.tasks if t["id"] == task_id), None)

    def writes(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.method in ("POST", "PATCH")]

    def _handle(self, req: httpx.Request) -> httpx.Response:
        self.requests.append(req)
        if self.down:
            raise httpx.ConnectError(f"connection refused ({SECRET})")  # worst case: leaks
        if self.always is not None:
            return httpx.Response(self.always, json={"error": "scripted"})
        if self.fail_next:
            f = self.fail_next.pop(0)
            if isinstance(f, Exception):
                raise f
            return httpx.Response(int(f), json={"error": "scripted"})
        path = req.url.path
        board = req.url.params.get("board")
        body = json.loads(req.content) if req.content else {}
        if req.method == "GET" and path == f"{PLUGIN}/board":
            snap = [dict(t) for t in self.tasks if t["board"] == board]  # state at request time
            resp = httpx.Response(200, json={"board": board, "tasks": snap})
            return self._gated(resp) if self.gate is not None else resp
        if req.method == "POST" and path == f"{PLUGIN}/tasks":
            self._ids += 1
            task = {**body, "id": f"t_{self._ids}", "board": board, "updated_at": self._tick()}
            self.tasks.append(task)
            return httpx.Response(201, json=task)
        parts = path.removeprefix(f"{PLUGIN}/tasks/").split("/")
        task = self.by_id(parts[0])
        if task is None:
            return httpx.Response(404, json={"error": "no such task"})
        if req.method == "PATCH" and len(parts) == 1:
            task.update(body)
            task["updated_at"] = self._tick()
            return httpx.Response(200, json=dict(task))
        if req.method == "POST" and parts[1:] == ["comments"]:
            self._ids += 1
            c = {"id": f"c_{self._ids}", "task_id": task["id"], "body": body["body"]}
            self.comments.append(c)
            return httpx.Response(201, json=c)
        return httpx.Response(405)

    async def _gated(self, resp: httpx.Response) -> httpx.Response:
        self.gate_waiting.set()
        assert self.gate is not None
        await self.gate.wait()
        return resp

    # human on the dashboard
    def human_edit(self, task_id: str, **fields) -> dict:
        task = self.by_id(task_id)
        assert task is not None
        task.update(fields)
        task["updated_at"] = self._tick()
        return self.event("task.updated", task=dict(task))

    def human_comment(self, task_id: str, body: str) -> dict:
        self._ids += 1
        return self.event(
            "comment.created", task_id=task_id, comment={"id": f"c_{self._ids}", "body": body}
        )

    def echo_of_last(self) -> dict:
        """What Hermes broadcasts after our own last write."""
        return self.event("task.updated", task=dict(self.tasks[-1]))

    def echo_of(self, task_snapshot: dict) -> dict:
        return self.event("task.updated", task=dict(task_snapshot))

    def event(self, type_: str, **body) -> dict:
        return {"id": len(self.ws_log) + 1, "type": type_, **body}

    # WS
    def emit_ws(self, ev: dict) -> None:
        self.ws_log.append(ev)
        for c in self.conns:
            if not c.closed:
                c.queue.put_nowait(json.dumps(ev))

    async def connect(self, url: str, headers: dict) -> FakeWs:
        if self.down:
            raise OSError(f"connect refused ({SECRET})")
        conn = FakeWs(url, headers)
        since = parse_qs(urlsplit(url).query).get("since", ["0"])[0]
        for ev in self.ws_log:  # replay what the client has not seen
            if ev["id"] > int(since or 0):
                conn.queue.put_nowait(json.dumps(ev))
        self.conns.append(conn)
        return conn

    async def settled(self) -> None:
        """Every open connection has handed over all its messages and waits for more."""
        for _ in range(500):
            live = [c for c in self.conns if not c.closed]
            if live and all(c.queue.empty() and c.idle.is_set() for c in live):
                return
            await asyncio.sleep(0.005)
        raise AssertionError("fake WS never settled")


class FakeRuns:
    def __init__(self) -> None:
        self.runs: dict[str, tuple[WorkerRun, WorkOrder]] = {}
        self.results: dict[str, dict] = {}

    def add(self, run_id: str, status: str, order: WorkOrder) -> None:
        run = WorkerRun(
            id=run_id, order_id=order["id"], variant={}, status=status,  # type: ignore[typeddict-item]
            started_at=None, ended_at=None, human_intervened=False,  # type: ignore[typeddict-item]
        )  # fmt: skip
        self.runs[run_id] = (run, order)

    def get(self, run_id):
        return self.runs[run_id][0]

    def result(self, run_id):
        return self.results.get(run_id)

    def running(self):
        return [r for r, _ in self.runs.values() if r["status"] in ("running", "waiting_human")]

    def order(self, run_id):
        hit = self.runs.get(run_id)
        return hit[1] if hit else None


# --- fixtures -------------------------------------------------------------------------


def make_project() -> Project:
    return Project(
        id=PID, slug=BOARD, name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": []},
        strategy="", policies=[], autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[], artifacts=[], channel="telegram:-100123:42", board=BOARD,
        default_profile="work",
        default_variant={"harness": "own", "engine": "claude", "browser": "chrome",
                         "context_policy": "default", "model": None},
        weight=2, watches=[],
    )  # fmt: skip


def make_order(**kw) -> WorkOrder:
    o = WorkOrder(
        id="wo_1", project_id=PID, kind="discover", intent="경쟁사 새 기능을 찾는다",
        done_criteria=["후보 3개"], budget={"max_minutes": 60}, profile="work", priority=1.0,
        value=3, due=None, backlog_item_id=None, action_mode="guarded",
    )  # fmt: skip
    o.update(kw)  # type: ignore[typeddict-item]
    return o


def card(**kw) -> CardView:
    base = dict(key="bl_1", board=BOARD, title="C사 가격 확인", body="의도", status="ready",
                priority=1, links=[], attachments=[])  # fmt: skip
    base.update(kw)
    return CardView(**base)  # type: ignore[typeddict-item]


@pytest.fixture
def fake_hermes():
    return FakeHermes()


@pytest.fixture
def projects(db, log, clock, tmp_path):
    store = SqliteProjectStore(db, log, tmp_path / "home", clock)
    store.create(make_project())
    return store


@pytest.fixture
def runs():
    return FakeRuns()


def make_mirror(db, log, clock, projects, fake, **kw) -> HermesKanbanMirror:
    return HermesKanbanMirror(
        BASE, db, log, clock, projects=projects, transport=fake.transport,
        ws_connect=fake.connect, ws_retry_s=(0.01, 0.05), **kw,
    )  # fmt: skip


@pytest.fixture
async def mirror(db, log, clock, projects, fake_hermes):
    m = make_mirror(db, log, clock, projects, fake_hermes)
    yield m
    await m.aclose()


@contextlib.asynccontextmanager
async def listening(m: HermesKanbanMirror, fake: FakeHermes):
    stop = asyncio.Event()
    task = asyncio.create_task(m.listen(stop))
    try:
        for _ in range(200):
            if fake.conns:
                break
            await asyncio.sleep(0.005)
        await fake.settled()
        yield
    finally:
        stop.set()
        await asyncio.wait_for(task, 2)


def item(projects, status="ready", priority=1, title="C사 가격 확인") -> BacklogItem:
    bid = projects.add_backlog(
        PID,
        BacklogItem(id="", title=title, intent="가격 페이지를 본다", done_criteria=["스샷"],
                    status=status, priority=priority, value=3),
    )  # fmt: skip
    return next(b for b in projects.get(PID)["backlog"] if b["id"] == bid)


def kinds(log, kind):
    return [e for e in log.query(kinds=[kind])]


async def created(mirror, fake, c: CardView) -> dict:
    mirror.upsert(c)
    await mirror.flush()
    return next(t for t in fake.tasks if t["title"] == c["title"])


# --- outbound: status mapping, no assignee --------------------------------------------


async def test_status_mapping(mirror, fake_hermes):
    mirror.upsert(card(status="blocked"))
    await mirror.flush()
    assert fake_hermes.tasks[0]["status"] == "blocked" and "assignee" not in fake_hermes.tasks[0]
    req = fake_hermes.writes()[0]
    assert req.url.path == f"{PLUGIN}/tasks" and req.url.params["board"] == BOARD
    assert req.headers["Idempotency-Key"] == "bl_1"  # CardView.key is Hermes's idempotency key
    assert TOKEN_HEADER not in req.headers  # no auth header unless configured


async def test_update_patches_the_mapped_task_without_assignee(mirror, fake_hermes):
    t = await created(mirror, fake_hermes, card(status="ready"))
    mirror.upsert(card(status="running", body="실행 중"))
    await mirror.flush()
    patch = fake_hermes.writes()[-1]
    assert patch.method == "PATCH" and patch.url.path == f"{PLUGIN}/tasks/{t['id']}"
    assert "assignee" not in json.loads(patch.content)
    assert len(fake_hermes.tasks) == 1 and fake_hermes.tasks[0]["status"] == "running"


async def test_unchanged_upsert_sends_nothing(mirror, fake_hermes):
    await created(mirror, fake_hermes, card())
    mirror.upsert(card())
    await mirror.flush()
    assert len(fake_hermes.writes()) == 1


@pytest.mark.parametrize(
    ("status", "want"),
    [("triage", "triage"), ("todo", "todo"), ("ready", "ready"), ("running", "running"),
     ("blocked", "blocked"), ("review", "review"), ("done", "done"), ("archived", "archived"),
     ("dropped", "archived")],
)  # fmt: skip
def test_card_from_backlog_status_table(status, want):
    it = BacklogItem(id="bl_9", title="t", intent="의도", done_criteria=["기준 A"],
                     status=status, priority=4, value=3)  # type: ignore[typeddict-item]  # fmt: skip
    c = card_from_backlog(it, make_project())
    assert c["status"] == want and c["key"] == "bl_9" and c["board"] == BOARD
    assert c["priority"] == 4 and "기준 A" in c["body"] and len(c["body"]) <= 4000


@pytest.mark.parametrize(
    ("status", "want"),
    [("leased", "running"), ("running", "running"), ("verifying", "running"),
     ("waiting_human", "blocked"), ("done", "done"), ("cancelled", "archived"),
     ("failed", "blocked"), ("interrupted", "ready")],
)  # fmt: skip
def test_card_from_run_status_table(status, want, runs):
    runs.add("run_1", status, make_order())
    run, order = runs.runs["run_1"]
    c = card_from_run(run, order, make_project())
    assert c["status"] == want and c["key"] == "run_1" and "경쟁사 새 기능" in c["title"]


def test_body_is_capped_at_4000_chars():
    it = BacklogItem(id="bl_9", title="t", intent="가" * 9000, done_criteria=[], status="ready",
                     priority=0, value=3)  # fmt: skip
    assert len(card_from_backlog(it, make_project())["body"]) <= 4000


# --- inbound: echoes, canonical order --------------------------------------------------


async def test_echo_of_own_write_is_ignored(mirror, fake_hermes):
    async with listening(mirror, fake_hermes):
        mirror.upsert(card(key="bl_1", status="ready"))
        await mirror.flush()
        fake_hermes.emit_ws(fake_hermes.echo_of_last())
        await fake_hermes.settled()
    assert list(mirror.inbound()) == []
    assert fake_hermes.tasks[0]["status"] == "ready" and len(fake_hermes.writes()) == 1


async def test_late_echo_of_an_older_write_is_not_a_human_move(mirror, fake_hermes, log):
    t = await created(mirror, fake_hermes, card(status="ready"))
    older = dict(t)  # our first write (ready) ...
    mirror.upsert(card(status="running"))
    await mirror.flush()  # ... then running
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.echo_of(older))  # ready arrives after running
        await fake_hermes.settled()
    assert list(mirror.inbound()) == [] and kinds(log, "mirror.reverted") == []


async def test_human_move_up_becomes_reorder_change(mirror, fake_hermes, projects, clock):
    it = item(projects, priority=1)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    writes = len(fake_hermes.writes())
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], priority=9))
        await fake_hermes.settled()
    [ch] = list(mirror.inbound())
    assert ch == {"card_key": it["id"], "kind": "reordered", "to_status": None, "priority": 9,
                  "comment": None}  # fmt: skip
    ev = change_event(ch, project_id=PID, at=clock.now())
    assert ev is not None and ev["kind"] == "human.kanban_reorder" and ev["actor"] == "human"
    assert ev["payload"] == {"card_key": it["id"], "priority": 9} and ev["project_id"] == PID
    await mirror.flush()  # allowed: a proposal for the Coordinator, not reverted
    assert len(fake_hermes.writes()) == writes and fake_hermes.by_id(t["id"])["priority"] == 9


async def test_forced_running_to_done_is_reverted(mirror, fake_hermes, log, clock):
    t = await created(mirror, fake_hermes, card(key="bl_1", status="running"))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="done"))
        await fake_hermes.settled()
    [ch] = list(mirror.inbound())
    assert ch["kind"] == "moved" and ch["to_status"] == "done" and ch["card_key"] == "bl_1"
    assert change_event(ch, project_id=PID, at=clock.now()) is None  # never submitted
    await mirror.flush()
    assert fake_hermes.by_id(t["id"])["status"] == "running"  # canonical status restored
    [rev] = kinds(log, "mirror.reverted")
    assert rev["payload"]["key"] == "bl_1"
    assert rev["payload"]["human_status"] == "done" and rev["payload"]["status"] == "running"


@pytest.mark.parametrize(
    ("canon", "human"),
    [("ready", "todo"), ("ready", "running"), ("todo", "done"), ("blocked", "review"),
     ("done", "todo")],
)  # fmt: skip
async def test_other_status_moves_are_reverted(mirror, fake_hermes, projects, canon, human):
    it = item(projects, status=canon)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status=human))
        await fake_hermes.settled()
    assert [c["kind"] for c in mirror.inbound()] == ["moved"]
    await mirror.flush()
    assert fake_hermes.by_id(t["id"])["status"] == canon


async def test_revert_war_stops_after_budget_and_alerts_once(db, log, clock, projects, fake_hermes):
    alerts = []

    async def alert(project_id, text):
        alerts.append((project_id, text))

    m = make_mirror(db, log, clock, projects, fake_hermes, alert=alert)
    it = item(projects, status="running")
    t = await created(m, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(m, fake_hermes):
        for _ in range(5):  # a second writer keeps dragging the card to done
            fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="done"))
            await fake_hermes.settled()
            await m.flush()
    await asyncio.sleep(0.01)  # the alert is sent in the background
    assert len(kinds(log, "mirror.reverted")) == 3
    [contested] = kinds(log, "mirror.contested")
    assert contested["payload"]["key"] == it["id"] and contested["project_id"] == PID
    assert len(alerts) == 1 and alerts[0][0] == PID and it["title"] in alerts[0][1]
    assert [c["kind"] for c in m.inbound()] == ["moved"] * 3
    assert len([r for r in fake_hermes.writes() if r.method == "PATCH"]) == 3
    assert fake_hermes.by_id(t["id"])["status"] == "done"  # left alone after the budget
    await m.resync(PID)  # the next resync retries
    assert fake_hermes.by_id(t["id"])["status"] == "running"
    assert len(kinds(log, "mirror.contested")) == 1 and len(alerts) == 1
    await m.aclose()


async def test_reverts_older_than_the_window_no_longer_count_toward_the_budget(
    mirror, fake_hermes, projects, log, clock
):
    it = item(projects, status="running")
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(mirror, fake_hermes):
        for _ in range(3):  # the whole budget, spent
            fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="done"))
            await fake_hermes.settled()
            await mirror.flush()
        clock.advance(REVERT_WINDOW + timedelta(seconds=1))
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="done"))
        await fake_hermes.settled()
        await mirror.flush()
    assert len(kinds(log, "mirror.reverted")) == 4  # put back again, not left alone
    assert kinds(log, "mirror.contested") == []
    assert fake_hermes.by_id(t["id"])["status"] == "running"


async def test_human_archive_of_backlog_item_is_proposed(mirror, fake_hermes, projects, clock):
    it = item(projects)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="archived"))
        await fake_hermes.settled()
    [ch] = list(mirror.inbound())
    assert ch["kind"] == "archived" and ch["to_status"] == "archived"
    ev = change_event(ch, project_id=PID, at=clock.now())
    assert ev["kind"] == "human.kanban_archive" and ev["payload"] == {"card_key": it["id"]}
    await mirror.flush()
    assert fake_hermes.by_id(t["id"])["status"] == "archived"  # left for the Coordinator


async def test_archive_of_a_run_card_is_reverted(mirror, fake_hermes, runs):
    runs.add("run_7", "running", make_order())
    run, order = runs.runs["run_7"]
    t = await created(mirror, fake_hermes, card_from_run(run, order, make_project()))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="archived"))
        await fake_hermes.settled()
    assert [c["kind"] for c in mirror.inbound()] == ["moved"]  # no backlog item to drop
    await mirror.flush()
    assert fake_hermes.by_id(t["id"])["status"] == "running"


async def test_human_comment_becomes_comment_change(mirror, fake_hermes, projects, clock):
    it = item(projects)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_comment(t["id"], "가격 말고 기능 비교도"))
        await fake_hermes.settled()
    [ch] = list(mirror.inbound())
    assert ch["kind"] == "commented" and ch["comment"] == "가격 말고 기능 비교도"
    ev = change_event(ch, project_id=PID, at=clock.now())
    assert ev["kind"] == "human.kanban_comment"
    assert ev["payload"] == {"card_key": it["id"], "comment": "가격 말고 기능 비교도"}


# --- stale echoes / snapshots never become human reorders ---


async def test_echo_while_canonical_changes_in_flight_is_not_a_reorder(
    mirror, fake_hermes, projects, db
):
    it = item(projects, priority=1)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    p1 = dict(t)
    db.execute("DELETE FROM kanban_echoes")  # its echo record never matched
    projects.set_backlog_priority(it["id"], 2)
    mirror.upsert(card_from_backlog({**it, "priority": 2}, projects.get(PID)))  # queued
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.echo_of(p1))  # arrives while p2 is still queued
        await fake_hermes.settled()
        await mirror.flush()
        fake_hermes.emit_ws(fake_hermes.echo_of(p1))  # and again after p2 went out: older
        await fake_hermes.settled()
    assert list(mirror.inbound()) == []
    assert fake_hermes.by_id(t["id"])["priority"] == 2


async def test_echo_key_includes_priority(mirror, fake_hermes, projects):
    it = item(projects, priority=1)
    fake_hermes.frozen_time = True  # our write and the human's share one updated_at
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], priority=9))
        await fake_hermes.settled()
    assert [(c["kind"], c["priority"]) for c in mirror.inbound()] == [("reordered", 9)]


async def test_echo_matches_a_differently_formatted_updated_at(mirror, fake_hermes, log):
    t = await created(mirror, fake_hermes, card(status="ready"))
    first = dict(t)
    mirror.upsert(card(status="running"))
    await mirror.flush()
    zulu = (
        datetime.fromisoformat(first["updated_at"]).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.echo_of({**first, "updated_at": zulu}))
        await fake_hermes.settled()
    assert list(mirror.inbound()) == [] and kinds(log, "mirror.reverted") == []


async def test_an_echo_is_consumed_so_a_replay_of_it_is_a_human_move(mirror, fake_hermes, projects):
    it = item(projects, priority=1)
    fake_hermes.frozen_time = True  # every state carries the same updated_at
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    first = dict(t)
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.echo_of(first))  # our write coming back: taken
        await fake_hermes.settled()
        projects.set_backlog_priority(it["id"], 2)
        mirror.upsert(card_from_backlog({**it, "priority": 2}, projects.get(PID)))
        await mirror.flush()
        fake_hermes.emit_ws(fake_hermes.echo_of_last())
        await fake_hermes.settled()
        fake_hermes.emit_ws(fake_hermes.echo_of(first))  # the same state again: not ours now
        await fake_hermes.settled()
    assert [(c["kind"], c["priority"]) for c in mirror.inbound()] == [("reordered", 1)]


async def test_an_echo_arriving_while_a_newer_write_is_queued_is_still_consumed(
    mirror, fake_hermes, projects
):
    it = item(projects, priority=1)
    fake_hermes.frozen_time = True  # every state carries the same updated_at
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    first = dict(t)
    projects.set_backlog_priority(it["id"], 2)
    mirror.upsert(card_from_backlog({**it, "priority": 2}, projects.get(PID)))  # queued
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.echo_of(first))  # arrives while p2 is still queued
        await fake_hermes.settled()
        await mirror.flush()
        fake_hermes.emit_ws(fake_hermes.echo_of_last())
        await fake_hermes.settled()
        fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], priority=1))
        await fake_hermes.settled()
    assert [(c["kind"], c["priority"]) for c in mirror.inbound()] == [("reordered", 1)]


async def test_resync_board_read_racing_a_flush_is_not_a_reorder(mirror, fake_hermes, projects):
    it = item(projects, priority=1)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    projects.set_backlog_priority(it["id"], 2)
    mirror.upsert(card_from_backlog({**it, "priority": 2}, projects.get(PID)))  # queued
    fake_hermes.gate = asyncio.Event()
    rs = asyncio.create_task(mirror.resync(PID))
    await asyncio.wait_for(fake_hermes.gate_waiting.wait(), 2)  # board read holds p1 now
    fl = asyncio.create_task(mirror.flush())  # the flush loop wakes meanwhile
    await asyncio.sleep(0.02)
    fake_hermes.gate.set()
    await asyncio.wait_for(asyncio.gather(rs, fl), 2)
    assert list(mirror.inbound()) == []  # the p1 snapshot is not a human move down
    assert fake_hermes.by_id(t["id"])["priority"] == 2


async def test_resync_board_read_racing_a_flush_waits_for_the_flush_lock(
    mirror, fake_hermes, projects
):
    """With frozen timestamps the stale-snapshot check cannot tell p1 from our p2 write, so
    only the flush lock keeps the flush from landing between the board read and the
    comparison."""
    fake_hermes.frozen_time = True
    it = item(projects, priority=1)
    t = await created(mirror, fake_hermes, card_from_backlog(it, projects.get(PID)))
    projects.set_backlog_priority(it["id"], 2)
    mirror.upsert(card_from_backlog({**it, "priority": 2}, projects.get(PID)))  # queued
    fake_hermes.gate = asyncio.Event()
    rs = asyncio.create_task(mirror.resync(PID))
    await asyncio.wait_for(fake_hermes.gate_waiting.wait(), 2)  # board read holds p1 now
    fl = asyncio.create_task(mirror.flush())  # the flush loop wakes meanwhile
    await asyncio.sleep(0.02)
    fake_hermes.gate.set()
    await asyncio.wait_for(asyncio.gather(rs, fl), 2)
    assert list(mirror.inbound()) == []  # the p1 snapshot is not a human move down
    assert fake_hermes.by_id(t["id"])["priority"] == 2


async def test_human_comment_is_handled_once_across_replay_and_edits(
    db, log, clock, projects, fake_hermes
):
    m = make_mirror(db, log, clock, projects, fake_hermes)
    it = item(projects)
    t = await created(m, fake_hermes, card_from_backlog(it, projects.get(PID)))
    ev = fake_hermes.human_comment(t["id"], "기능 비교도")
    cid = ev["comment"]["id"]
    async with listening(m, fake_hermes):
        fake_hermes.emit_ws(ev)
        fake_hermes.emit_ws({**ev, "id": ev["id"] + 1})  # same comment delivered again
        for typ, body in (("comment.updated", "고쳤어요"), ("comment.deleted", "")):
            fake_hermes.emit_ws(fake_hermes.event(typ, task_id=t["id"],
                                                   comment={"id": cid, "body": body}))  # fmt: skip
        await fake_hermes.settled()
    assert [c["comment"] for c in m.inbound()] == ["기능 비교도"]
    await m.aclose()
    db.execute("DELETE FROM kanban_kv WHERE key='ws_since'")  # restart that replays history
    m2 = make_mirror(db, log, clock, projects, fake_hermes)
    async with listening(m2, fake_hermes):
        pass
    assert list(m2.inbound()) == []
    await m2.aclose()


async def test_events_for_cards_we_do_not_own_are_ignored(mirror, fake_hermes):
    fake_hermes.tasks.append({"id": "t_foreign", "board": BOARD, "status": "todo", "priority": 0})
    async with listening(mirror, fake_hermes):
        fake_hermes.emit_ws(fake_hermes.human_edit("t_foreign", status="done"))
        fake_hermes.emit_ws({"id": 99, "type": "board.renamed"})
        await fake_hermes.settled()
    assert list(mirror.inbound()) == [] and fake_hermes.writes() == []


# --- dashboard down: durable queue, backoff, resync -----------------------------------


async def test_queue_while_dashboard_down_then_resync(mirror, fake_hermes, projects, log, clock):
    a = item(projects, status="running", title="A")
    ta = await created(mirror, fake_hermes, card_from_backlog(a, projects.get(PID)))
    fake_hermes.down = True
    b = item(projects, status="ready", title="B")
    mirror.upsert(card_from_backlog(b, projects.get(PID)))  # returns at once, never raises
    await mirror.flush()
    assert [t["title"] for t in fake_hermes.tasks] == ["A"]
    assert kinds(log, "mirror.retry")
    ta["status"] = "done"  # a human dragged A while we could not hear the board
    fake_hermes.down = False
    await mirror.resync(PID)
    assert {t["title"]: t["status"] for t in fake_hermes.tasks} == {"A": "running", "B": "ready"}
    [rev] = kinds(log, "mirror.reverted")
    assert rev["payload"]["key"] == a["id"] and rev["payload"]["human_status"] == "done"
    [rs] = kinds(log, "mirror.resync")
    assert rs["payload"]["board"] == BOARD and rs["project_id"] == PID


async def test_resync_reports_allowed_offline_changes_first(mirror, fake_hermes, projects):
    a = item(projects, title="A", priority=1)
    b = item(projects, title="B")
    ta = await created(mirror, fake_hermes, card_from_backlog(a, projects.get(PID)))
    tb = await created(mirror, fake_hermes, card_from_backlog(b, projects.get(PID)))
    ta["priority"] = 7  # offline: moved up
    tb["status"] = "archived"  # offline: thrown away
    await mirror.resync(PID)
    got = {c["card_key"]: c["kind"] for c in mirror.inbound()}
    assert got == {a["id"]: "reordered", b["id"]: "archived"}
    assert ta["priority"] == 7 and tb["status"] == "archived"  # proposals, not overwritten


async def test_resync_rebuilds_cards_from_the_canonical_store(mirror, fake_hermes, projects):
    a = item(projects, status="todo", title="A")
    b = item(projects, status="ready", title="B")
    projects.update_backlog(b["id"], status="dropped")
    await mirror.resync(PID)
    assert {t["title"]: t["status"] for t in fake_hermes.tasks} == {"A": "todo", "B": "archived"}
    assert all("assignee" not in t for t in fake_hermes.tasks)
    assert a["id"] in [r.headers["Idempotency-Key"] for r in fake_hermes.writes()]


async def test_resync_recreates_a_task_deleted_on_the_board(mirror, fake_hermes, projects):
    a = item(projects, title="A")
    ta = await created(mirror, fake_hermes, card_from_backlog(a, projects.get(PID)))
    fake_hermes.tasks.remove(ta)
    await mirror.resync(PID)
    [again] = fake_hermes.tasks
    assert again["title"] == "A" and again["id"] != ta["id"]


async def test_queue_survives_restart_and_honors_backoff(db, log, clock, projects, fake_hermes):
    m1 = make_mirror(db, log, clock, projects, fake_hermes)
    fake_hermes.down = True
    m1.upsert(card(status="todo"))
    await m1.flush()
    await m1.aclose()  # daemon restarts
    fake_hermes.down = False
    m2 = make_mirror(db, log, clock, projects, fake_hermes)
    await m2.flush()
    assert fake_hermes.tasks == []  # still inside the backoff window
    clock.advance(timedelta(minutes=11))
    await m2.flush()
    assert [t["status"] for t in fake_hermes.tasks] == ["todo"]
    await m2.aclose()


async def test_backoff_grows_and_is_capped(mirror, fake_hermes, log, clock):
    fake_hermes.down = True
    mirror.upsert(card())
    waits = []
    for _ in range(7):
        await mirror.flush(force=True)
        waits.append(kinds(log, "mirror.retry")[-1]["payload"]["wait_s"])
    assert waits[:3] == [30, 60, 120] and max(waits) == 600


async def test_recovery_is_declared_only_after_reaching_hermes(mirror, fake_hermes, log, clock):
    fake_hermes.down = True
    await mirror.resync(PID)  # board read fails: backoff, nothing queued
    assert kinds(log, "mirror.retry")
    clock.advance(timedelta(minutes=1))
    await mirror.flush()  # nothing to send proves nothing
    assert kinds(log, "mirror.recovered") == []
    fake_hermes.down = False
    await mirror.resync(PID)
    assert len(kinds(log, "mirror.recovered")) == 1


async def test_404_on_patch_recreates_the_task(mirror, fake_hermes):
    t = await created(mirror, fake_hermes, card(status="ready"))
    fake_hermes.tasks.remove(t)
    mirror.upsert(card(status="running"))
    await mirror.flush()
    assert [x["status"] for x in fake_hermes.tasks] == ["running"]


async def test_rejected_card_is_dead_lettered_not_blocking(mirror, fake_hermes, log):
    fake_hermes.fail_next = [422]
    mirror.upsert(card(key="bl_bad", title="bad"))
    mirror.upsert(card(key="bl_ok", title="ok"))
    await mirror.flush()
    assert [t["title"] for t in fake_hermes.tasks] == ["ok"]
    [dead] = kinds(log, "mirror.dead")
    assert dead["payload"]["key"] == "bl_bad" and dead["payload"]["status"] == 422


async def test_board_change_archives_the_task_on_the_old_board(mirror, fake_hermes):
    old = await created(mirror, fake_hermes, card(board=BOARD))
    mirror.upsert(card(board="other"))
    await mirror.flush()
    assert old["status"] == "archived"  # not orphaned on the old board
    [new] = [t for t in fake_hermes.tasks if t["board"] == "other"]
    assert new["status"] == "ready" and new["id"] != old["id"]


async def test_dead_lettered_comment_does_not_count_as_recovery(mirror, fake_hermes, log):
    await created(mirror, fake_hermes, card())
    mirror.comment("bl_1", "안녕", ref="r1")
    fake_hermes.down = True
    await mirror.flush()  # the comment cannot go out: backoff
    fake_hermes.down = False
    fake_hermes.fail_next = [422]
    await mirror.flush(force=True)
    assert kinds(log, "mirror.recovered") == []


async def test_auth_failure_alerts_once_without_retry_spam(db, log, clock, projects, fake_hermes):
    alerts = []

    async def alert(project_id, text):
        alerts.append(text)

    m = make_mirror(db, log, clock, projects, fake_hermes, alert=alert)
    fake_hermes.always = 401
    m.upsert(card())
    for _ in range(3):
        await m.flush(force=True)
        await m.resync(PID)
    await asyncio.sleep(0.01)
    assert len(kinds(log, "mirror.auth_failed")) == 1 and len(alerts) == 1
    assert kinds(log, "mirror.retry") == []
    fake_hermes.always = None
    await m.flush(force=True)
    assert [t["status"] for t in fake_hermes.tasks] == ["ready"]
    fake_hermes.always = 403  # a new episode after a recovery alerts again
    m.upsert(card(status="running"))
    await m.flush(force=True)
    await asyncio.sleep(0.01)
    assert len(kinds(log, "mirror.auth_failed")) == 2 and len(alerts) == 2
    await m.aclose()


# --- WS resume, token ------------------------------------------------------------------


async def test_ws_resumes_from_persisted_since_id(db, log, clock, projects, fake_hermes):
    m1 = make_mirror(db, log, clock, projects, fake_hermes)
    t = await created(m1, fake_hermes, card(status="ready", priority=1))
    async with listening(m1, fake_hermes):
        first = fake_hermes.human_comment(t["id"], "첫 댓글")
        fake_hermes.emit_ws(first)
        await fake_hermes.settled()
    assert [c["comment"] for c in m1.inbound()] == ["첫 댓글"]
    await m1.aclose()
    fake_hermes.emit_ws(fake_hermes.human_comment(t["id"], "꺼져 있을 때 단 댓글"))
    m2 = make_mirror(db, log, clock, projects, fake_hermes)  # after a restart
    async with listening(m2, fake_hermes):
        pass
    url = fake_hermes.conns[-1].url
    assert url.startswith("ws://127.0.0.1:9119/api/plugins/kanban/events")
    assert parse_qs(urlsplit(url).query)["since"] == [str(first["id"])]
    assert [c["comment"] for c in m2.inbound()] == ["꺼져 있을 때 단 댓글"]  # not "첫 댓글"
    await m2.aclose()


async def test_ws_reconnects_after_drop(mirror, fake_hermes):
    t = await created(mirror, fake_hermes, card())
    async with listening(mirror, fake_hermes):
        fake_hermes.conns[0].queue.put_nowait(ConnectionError("dropped"))
        for _ in range(200):
            if len(fake_hermes.conns) > 1:
                break
            await asyncio.sleep(0.005)
        fake_hermes.emit_ws(fake_hermes.human_comment(t["id"], "다시 연결됨"))
        await fake_hermes.settled()
    assert len(fake_hermes.conns) == 2
    assert [c["comment"] for c in mirror.inbound()] == ["다시 연결됨"]


async def test_session_token_sent_and_never_logged(db, log, clock, projects, fake_hermes, caplog):
    caplog.set_level(logging.DEBUG)
    m = make_mirror(db, log, clock, projects, fake_hermes, token=SECRET)
    t = await created(m, fake_hermes, card())
    assert fake_hermes.writes()[0].headers[TOKEN_HEADER] == SECRET
    async with listening(m, fake_hermes):
        pass
    assert fake_hermes.conns[0].headers[TOKEN_HEADER] == SECRET
    assert SECRET not in fake_hermes.conns[0].url  # header only, never in a URL
    fake_hermes.down = True  # errors that carry the token in their text
    m.upsert(card(status="running"))
    await m.flush()
    await m.resync(PID)
    stop = asyncio.Event()
    listener = asyncio.create_task(m.listen(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(listener, 2)
    assert t["id"]
    # what websockets / httpcore print at DEBUG (handshake headers) is scrubbed too
    logging.getLogger("websockets.client").debug("> %s: %s", TOKEN_HEADER, SECRET)
    logging.getLogger("httpcore.http11").debug("send_request_headers %s", SECRET)
    assert SECRET not in caplog.text
    assert "<token>" in caplog.text or kinds(log, "mirror.retry")
    for ev in log.query():
        assert SECRET not in json.dumps(ev["payload"], ensure_ascii=False)
    assert SECRET not in repr(m)
    await m.aclose()


# --- 14 days ---------------------------------------------------------------------------


async def test_done_archived_after_14_days(mirror, clock, fake_hermes, projects):
    it = item(projects)
    projects.update_backlog(it["id"], status="done")
    done = next(b for b in projects.get(PID)["backlog"] if b["id"] == it["id"])
    t = await created(mirror, fake_hermes, card_from_backlog(done, projects.get(PID)))
    assert t["status"] == "done"
    clock.advance(ARCHIVE_AFTER - timedelta(minutes=1))
    await mirror.resync(PID)
    assert fake_hermes.by_id(t["id"])["status"] == "done"
    clock.advance(timedelta(minutes=2))
    await mirror.resync(PID)
    assert fake_hermes.by_id(t["id"])["status"] == "archived"
    mirror.upsert(card_from_backlog(done, projects.get(PID)))  # still "done" in the store
    await mirror.flush()
    assert fake_hermes.by_id(t["id"])["status"] == "archived"  # stays archived


# --- feed: our events -> upserts ------------------------------------------------------


async def test_feed_mirrors_backlog_events(mirror, fake_hermes, projects):
    it = item(projects, status="todo", title="피드")
    await mirror.feed_once()
    await mirror.flush()
    assert [(t["title"], t["status"]) for t in fake_hermes.tasks] == [("피드", "todo")]
    projects.update_backlog(it["id"], status="running")
    projects.set_backlog_priority(it["id"], 5)
    await mirror.feed_once()
    await mirror.flush()
    assert fake_hermes.tasks[0]["status"] == "running" and fake_hermes.tasks[0]["priority"] == 5
    await mirror.feed_once()  # re-reading the same events changes nothing
    await mirror.flush()
    assert len(fake_hermes.writes()) == 2


async def test_feed_follows_log_order_not_timestamps(mirror, fake_hermes, projects, clock):
    item(projects, title="A")
    await mirror.feed_once()
    clock.advance(timedelta(seconds=-30))  # logged later, stamped earlier
    item(projects, title="B")
    await mirror.feed_once()
    await mirror.flush()
    assert sorted(t["title"] for t in fake_hermes.tasks) == ["A", "B"]


def run_event(log, clock, run_id, kind, payload=None):
    from swarm.types import Event

    log.append(
        Event(id="", ts=clock.now(), kind=kind, actor="workers", project_id=PID, run_id=run_id,
              payload=payload or {}, refs=[])
    )  # fmt: skip


async def test_feed_mirrors_runs_without_a_backlog_item(
    db, log, clock, projects, fake_hermes, runs
):
    m = make_mirror(db, log, clock, projects, fake_hermes, runs=runs)
    runs.add("run_d", "running", make_order())
    runs.add("run_i", "running", make_order(id="wo_2", backlog_item_id="bl_x", kind="advance"))
    run_event(log, clock, "run_d", "run.running")
    run_event(log, clock, "run_i", "run.running")
    await m.feed_once()
    await m.flush()
    assert [(t["status"]) for t in fake_hermes.tasks] == ["running"]  # item runs: item card
    assert fake_hermes.writes()[0].headers["Idempotency-Key"] == "run_d"
    runs.add("run_d", "done", make_order())
    run_event(log, clock, "run_d", "run.done")
    await m.feed_once()
    await m.flush()
    assert fake_hermes.tasks[0]["status"] == "done"
    await m.aclose()


async def test_item_card_shows_latest_summary_and_artifacts(
    db, log, clock, projects, fake_hermes, runs
):
    m = make_mirror(db, log, clock, projects, fake_hermes, runs=runs)
    it = item(projects, status="running")
    runs.add("run_s", "done", make_order(backlog_item_id=it["id"], kind="advance"))
    runs.results["run_s"] = {
        "run_id": "run_s", "status": "done", "summary": "C사 가격 9,900원 확인",
        "artifacts": [{"id": "art_1", "title": "보고서", "path": "/tmp/swarm/report.md",
                       "status": "verified", "verifier_run_id": "run_v",
                       "evidence": [{"url": "https://c.test/price", "screenshot": None,
                                     "captured_at": clock.now(), "text": None}]}],
        "cost": {}, "failure": None,
    }  # fmt: skip
    run_event(log, clock, "run_s", "run.finished", {"status": "done"})
    await m.feed_once()
    await m.flush()
    [task] = fake_hermes.tasks  # the run updates its item's card, no card of its own
    for want in ("C사 가격 9,900원 확인", "/tmp/swarm/report.md", "https://c.test/price"):
        assert want in task["body"]
    writes = len(fake_hermes.writes())
    await m.resync(PID)  # rebuilt from the store: the summary stays, nothing to rewrite
    assert len(fake_hermes.writes()) == writes
    projects.update_backlog(it["id"], status="blocked", note="실행 오류: TimeoutError")
    await m.feed_once()
    await m.flush()
    assert "실행 오류: TimeoutError" in task["body"] and "9,900원" not in task["body"]
    projects.update_backlog(it["id"], status="running", note=new_id("wo"))
    await m.feed_once()
    await m.flush()
    assert "실행 오류: TimeoutError" in task["body"] and "wo_" not in task["body"]
    await m.aclose()


async def test_ask_raised_comments_on_the_card_once(db, log, clock, projects, fake_hermes, runs):
    m = make_mirror(db, log, clock, projects, fake_hermes, runs=runs)
    it = item(projects, status="blocked")
    runs.add("run_a", "waiting_human", make_order(backlog_item_id=it["id"], kind="advance"))
    await m.feed_once()
    ask = {"ask_id": "ask_1", "kind": "choice", "number": 3, "channel": "telegram:-1:2"}
    run_event(log, clock, "run_a", "ask.raised", ask)
    for _ in range(2):  # the feed re-reads; the comment goes out once
        await m.feed_once()
        await m.flush()
    [c] = fake_hermes.comments
    assert c["task_id"] == fake_hermes.tasks[0]["id"] and "#3" in c["body"]
    async with listening(m, fake_hermes):  # our own comment echoes back: not a human one
        echo = {"id": c["id"], "body": c["body"]}
        fake_hermes.emit_ws(
            fake_hermes.event("comment.created", task_id=c["task_id"], comment=echo)
        )
        await fake_hermes.settled()
    assert list(m.inbound()) == []
    await m.aclose()


# --- background loop --------------------------------------------------


async def test_run_loop_resyncs_and_submits_human_changes(db, log, clock, projects, fake_hermes):
    it = item(projects, priority=1)
    m = make_mirror(db, log, clock, projects, fake_hermes)
    submitted = []

    async def submit(ev):
        submitted.append(ev)

    stop = asyncio.Event()
    task = asyncio.create_task(m.run(stop, submit=submit))
    for _ in range(400):
        if fake_hermes.tasks and fake_hermes.conns:
            break
        await asyncio.sleep(0.005)
    [t] = fake_hermes.tasks  # initial resync mirrored the backlog
    await fake_hermes.settled()
    fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], priority=8))
    fake_hermes.emit_ws(fake_hermes.human_edit(t["id"], status="done"))
    for _ in range(400):
        if submitted and fake_hermes.by_id(t["id"])["status"] == "ready":
            break
        await asyncio.sleep(0.005)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert [e["kind"] for e in submitted] == ["human.kanban_reorder"]
    assert submitted[0]["payload"] == {"card_key": it["id"], "priority": 8}
    assert fake_hermes.by_id(t["id"])["status"] == "ready"  # forced move reverted by the loop
    assert kinds(log, "mirror.inbound")


async def test_a_dying_loop_is_logged_and_restarted(db, log, clock, projects, fake_hermes):
    m = make_mirror(db, log, clock, projects, fake_hermes, loop_retry_s=(0.01, 0.05), token=SECRET)
    real, calls = m.listen, []

    async def flaky(stop, on_change=None):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError(f"database is locked ({SECRET})")
        await real(stop, on_change)

    m.listen = flaky  # type: ignore[method-assign]

    async def submit(ev):
        pass

    stop = asyncio.Event()
    task = asyncio.create_task(m.run(stop, submit=submit))
    for _ in range(400):
        if fake_hermes.conns:
            break
        await asyncio.sleep(0.005)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert fake_hermes.conns  # the WS loop came back
    [died] = kinds(log, "mirror.loop_died")
    assert died["payload"]["loop"] == "ws" and SECRET not in died["payload"]["error"]
    await m.aclose()


async def test_restart_never_re_proposes_a_submitted_change(db, log, clock, projects, fake_hermes):
    """The Coordinator persists submitted human.kanban_* events and replays
    them after a restart, so the mirror's start-up resync must not propose the same remote
    state again (proposal_json is durable; after the grace it is reverted, never re-proposed)."""
    a = item(projects, title="A", priority=1)
    m = make_mirror(db, log, clock, projects, fake_hermes)
    ta = await created(m, fake_hermes, card_from_backlog(a, projects.get(PID)))
    ta["priority"] = 7  # the human moved it up
    await m.resync(PID)
    assert [c["kind"] for c in m.inbound()] == ["reordered"]  # submitted once
    await m.aclose()
    m2 = make_mirror(db, log, clock, projects, fake_hermes)  # swarmd restarted
    await m2.resync(PID)
    assert list(m2.inbound()) == []
    clock.advance(timedelta(minutes=6))
    await m2.resync(PID)
    assert list(m2.inbound()) == []  # past the grace: reverted, not proposed again
    await m2.aclose()
