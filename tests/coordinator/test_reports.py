from datetime import datetime, timedelta

import pytest
from fakes import FakeBrain

from swarm.clock import SEOUL, FakeClock
from swarm.coordinator.reports import (
    ProjectNarration,
    ReportFacts,
    ReportNarration,
    build_daily,
    build_weekly,
    render_report,
    render_template,
)
from swarm.inbox.inbox import SqliteInbox
from swarm.ports import BrainError
from swarm.projects.store import SqliteProjectStore
from swarm.scheduler.ledger import SqliteQuotaLedger
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog
from swarm.types import KR, AskDraft, Event, Project, UsageSignal

NOW = datetime(2026, 11, 7, 8, 0, tzinfo=SEOUL)


def make_project() -> Project:
    return Project(
        id="prj_compet",
        slug="compet",
        name="경쟁사",
        goal={
            "objective": "경쟁사 변화를 먼저 안다",
            "key_results": [
                KR(id="kr_1", metric="주간 경쟁사 리포트", target=4, current=1, source=""),
                KR(id="kr_2", metric="추적 경쟁사 수", target=5, current=5, source=""),
            ],
        },
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


# Overrides the root clock: it starts 30 h before NOW so tests can walk into the report window.
@pytest.fixture
def clock():
    return FakeClock(NOW - timedelta(hours=30))


@pytest.fixture
def env(tmp_path, clock):
    db = Database(tmp_path / "swarm.db")
    log = SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))
    projects = SqliteProjectStore(db, log, tmp_path / "home", clock)
    pid = projects.create(make_project())
    inbox = SqliteInbox(
        db,
        log,
        projects,
        FakeBrain(),
        clock,
        run_project=lambda r: pid,
        general_channel="telegram:-100123:1",
    )
    ledger = SqliteQuotaLedger(db, log, clock, instance_id="inst_a")
    return log, projects, inbox, ledger, pid


def finished(log, pid, run_id, status, ts):
    log.append(
        Event(
            id="",
            ts=ts,
            kind="run.finished",
            actor="workers",
            project_id=pid,
            run_id=run_id,
            payload={"status": status},
            refs=[],
        )
    )


def draft(question) -> AskDraft:
    return AskDraft(
        kind="decision",
        question=question,
        options=["A안", "B안"],
        recommended="B안",
        default_after=None,
        irreversible=False,
        evidence=[],
    )


@pytest.fixture
async def facts(env, clock) -> ReportFacts:
    log, projects, inbox, ledger, pid = env
    finished(log, pid, "run_old", "done", NOW - timedelta(hours=26))  # outside the window
    clock.advance(timedelta(hours=10))  # NOW - 20h
    finished(log, pid, "run_1", "done", NOW - timedelta(hours=20))
    finished(log, pid, "run_2", "done", NOW - timedelta(hours=10))
    finished(log, pid, "run_3", "interrupted", NOW - timedelta(hours=9))
    finished(log, pid, "run_3", "failed", NOW - timedelta(hours=8))  # last status wins
    await inbox.raise_ask("run_2", draft("C사 대응안 A/B 중 어느 쪽?"))
    projects.record_learning(pid, {"text": "C사는 화요일에 가격을 바꾼다", "evidence": []})
    ledger.observe(
        engine="claude",
        signal=UsageSignal(kind="limit_hit", reset_at=NOW + timedelta(hours=3), at=clock.now()),
    )
    return build_daily(log, projects, inbox, ledger, NOW)


async def test_build_daily_counts_are_deterministic(facts, env):
    f = facts
    assert f.kind == "daily" and f.since == NOW - timedelta(hours=24) and f.until == NOW
    assert (f.runs_done, f.runs_failed) == (2, 1)
    assert f.quota_hits == 1 and f.cooling_until == NOW + timedelta(hours=3)
    [p] = f.projects
    assert p.name == "경쟁사" and (p.runs_done, p.runs_failed, p.learnings) == (2, 1, 1)
    assert [(k.metric, k.current, k.target) for k in p.krs] == [
        ("주간 경쟁사 리포트", 1, 4),
        ("추적 경쟁사 수", 5, 5),
    ]
    assert [(a.number, a.question) for a in f.open_asks] == [(1, "C사 대응안 A/B 중 어느 쪽?")]
    assert p.open_asks == f.open_asks


async def test_build_weekly_window(env):
    log, projects, inbox, ledger, _ = env
    f = build_weekly(log, projects, inbox, ledger, NOW)
    assert f.kind == "weekly" and f.since == NOW - timedelta(days=7)


async def test_report_numbers_come_from_facts_not_llm(facts):
    brain = FakeBrain()
    brain.result = ReportNarration(
        conclusion="어제 완료 99건으로 순항",  # made-up number -> dropped
        changed=["C사가 무료 티어를 줄였다"],
        done=["리포트 초안 작성", "가격표 37개 수집"],  # 37 is not a fact -> dropped
        projects=[ProjectNarration(project_id="prj_compet", health="위험", note="리포트 부족")],
    )
    text = await render_report(facts, brain)
    assert brain.calls[0]["purpose"] == "report"
    assert "99" not in text and "37" not in text
    lines = text.splitlines()
    assert lines[0].startswith("📊 일일 보고 · ")
    assert "완료 2" in text and "실패 1" in text
    assert "주간 경쟁사 리포트 1/4" in text and "추적 경쟁사 수 5/5" in text
    assert "C사가 무료 티어를 줄였다" in text and "리포트 초안 작성" in text
    assert "위험" in text
    # order: 결론 -> 바뀐 것 -> 한 것 -> 목표까지 -> 나한테 필요한 것
    idx = [text.index(h) for h in ("바뀐 것", "한 것", "목표까지", "나한테 필요한 것")]
    assert idx == sorted(idx)
    # "나한테 필요한 것" matches the open Ask list
    assert text.rstrip().endswith("- #1 C사 대응안 A/B 중 어느 쪽?")
    # the prompt carries the facts, and the narration schema has no numeric fields
    assert "완료 2" in brain.calls[0]["prompt"]


async def test_report_falls_back_to_template_on_brain_error(facts):
    brain = FakeBrain()
    brain.error = BrainError("engine unavailable")
    text = await render_report(facts, brain)
    lines = text.splitlines()
    assert lines[0].startswith("📊 일일 보고 · ")
    assert "완료 2" in lines[0] and "실패 1" in lines[0] and "열린 질문 1" in lines[0]
    assert "주간 경쟁사 리포트 1/4" in text
    assert "#1 C사 대응안 A/B 중 어느 쪽?" in text
    assert "순항" not in text and "위험" not in text  # health is the LLM's call: none here


async def test_all_on_track_report_is_at_most_five_lines(facts):
    brain = FakeBrain()
    brain.result = ReportNarration(
        conclusion="계획대로 진행 중",
        changed=["변화 없음"],
        done=["리포트 초안 작성"],
        projects=[ProjectNarration(project_id="prj_compet", health="순항", note="")],
    )
    text = await render_report(facts, brain)
    lines = text.splitlines()
    assert len(lines) <= 5
    assert "완료 2" in text and "#1 C사 대응안 A/B 중 어느 쪽?" in text
    assert "순항" in text
    assert lines[1] == "바뀐 것: 변화 없음"
    heads = ("바뀐 것:", "한 것:", "목표까지:", "나한테 필요한 것:")
    assert [next(i for i, ln in enumerate(lines) if ln.startswith(h)) for h in heads] == [
        1,
        2,
        3,
        4,
    ]


async def test_project_missing_from_narration_is_not_on_track(facts):
    brain = FakeBrain()
    brain.result = ReportNarration(conclusion="진행 중", changed=[], done=[], projects=[])
    text = await render_report(facts, brain)
    assert len(text.splitlines()) > 5  # unknown health -> full format
    assert "주간 경쟁사 리포트 1/4" in text


async def test_per_project_weekly_counts_engine_wide_quota(facts, env):
    log, projects, inbox, ledger, pid = env
    f = build_weekly(log, projects, inbox, ledger, NOW, project_id=pid)
    assert f.quota_hits == 1  # engine.quota_exhausted carries no project
    assert "엔진 한도(전체) 1회" in render_template(f)


async def test_date_numbers_in_prose_are_not_whitelisted(facts):
    brain = FakeBrain()
    brain.result = ReportNarration(
        conclusion="계획대로 진행 중",
        changed=["C사가 2026년 11월 7일에 공지했다", "C사가 무료 티어를 줄였다"],
        done=[],
        projects=[ProjectNarration(project_id="prj_compet", health="위험", note="")],
    )
    text = await render_report(facts, brain)
    assert "공지했다" not in text and "C사가 무료 티어를 줄였다" in text
