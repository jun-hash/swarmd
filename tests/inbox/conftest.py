from datetime import datetime

import pytest
from pydantic import BaseModel

from swarm.clock import SEOUL, FakeClock
from swarm.inbox.inbox import SqliteInbox
from swarm.ports import BrainError
from swarm.projects.store import SqliteProjectStore
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import AskDraft, Policy, Project


def make_project() -> Project:
    return Project(
        id="prj_compet",
        slug="compet",
        name="경쟁사",
        goal={"objective": "경쟁사 변화를 먼저 안다", "key_results": []},
        strategy="",
        policies=[],
        autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[],
        artifacts=[],
        channel="telegram:-100123:42",
        board="compet",
        default_profile="work",
        default_variant={
            "harness": "own",
            "engine": "claude",
            "browser": "chrome",
            "context_policy": "default",
            "model": None,
        },
        weight=2,
        watches=[],
    )


class CountingBrain:
    """Replays one scripted result and counts calls as an int, which the inbox tests assert on.

    It stays local because the shared FakeBrain records calls as a list."""

    def __init__(self):
        self.calls = 0
        self.prompts: list[str] = []
        self._result = None
        self.delay = 0.0

    def returns(self, result):
        self._result = result

    async def decide(self, *, purpose, prompt, schema: type[BaseModel], timeout_s=60):
        import asyncio

        self.calls += 1
        self.prompts.append(prompt)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self._result is None:
            raise BrainError("no result")
        return self._result


def draft(**kw) -> AskDraft:
    base = AskDraft(
        kind="decision",
        question="어느 계정으로 로그인할까?",
        options=["회사 계정 사용", "개인 계정 사용"],
        recommended="회사 계정 사용",
        default_after=None,
        irreversible=False,
        evidence=[],
    )
    base.update(kw)  # type: ignore[typeddict-item]
    return base


# Overrides the root clock with a late-evening start; timers in these tests run past midnight.
@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 11, 6, 22, 50, tzinfo=SEOUL))


@pytest.fixture
def fake_brain():
    return CountingBrain()


@pytest.fixture
def env(tmp_path, clock):
    db = Database(tmp_path / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    pid = projects.create(make_project())
    return db, log, projects, pid


@pytest.fixture
def projects(env):
    return env[2]


# Overrides the root log with the env's log, which shares the inbox's database.
@pytest.fixture
def log(env):
    return env[1]


@pytest.fixture
def pid(env):
    return env[3]


@pytest.fixture
def run_map(pid):
    return {"run_1": pid, "run_2": pid, "run_free": None}


@pytest.fixture
def inbox(env, fake_brain, clock, run_map):
    db, log, projects, _ = env
    return SqliteInbox(
        db,
        log,
        projects,
        fake_brain,
        clock,
        run_project=lambda r: run_map.get(r),
        general_channel="telegram:-100123:1",
    )


@pytest.fixture
def with_policy(projects, pid):
    projects.add_policy(pid, Policy(text="계정은 항상 회사 계정", source_ask=None))
    return projects.get(pid)["policies"][0]["id"]
