import sqlite3
from datetime import datetime

import pytest

from swarm.clock import SEOUL
from swarm.types import BacklogItem, Evidence, FailedAttempt, Learning, MemoryEntry, Policy

from .conftest import make_project


def ev():
    return Evidence(
        url="http://x", screenshot=None, captured_at=datetime(2026, 11, 6, tzinfo=SEOUL), text="t"
    )


def kinds(log):
    return [e["kind"] for e in log.query()]


def test_by_channel_maps_topic(store, project):
    assert store.by_channel("telegram:-100123:42")["id"] == project["id"]
    assert store.by_channel("telegram:-1:1") is None


def test_create_emits_event_and_renders(store, project, home, log):
    assert "project.created" in kinds(log)
    for f in ("GOAL", "POLICIES", "MEMORY"):
        assert (home / "projects" / project["slug"] / f"{f}.md").exists()
    assert store.list()[0]["id"] == project["id"]


def test_get_unknown_raises(store):
    with pytest.raises(KeyError):
        store.get("nope")


def test_policy_deactivate_keeps_history(store, project, log):
    pid = store.add_policy(project["id"], Policy(text="질문형이면 자율 게시", source_ask="ask_1"))
    store.deactivate_policy(pid, reason="사람이 파일에서 삭제")
    p = store.get(project["id"])
    assert [x["active"] for x in p["policies"] if x["id"] == pid] == [False]
    assert {"project.policy_added", "project.policy_deactivated"} <= set(kinds(log))


def test_memory_brief_respects_budget_and_split(store, project):
    for i in range(200):
        store.record_learning(project["id"], Learning(text=f"배운 것 {i} " * 20, evidence=[]))
        store.record_failure(
            project["id"], FailedAttempt(what=f"시도 {i} " * 20, why="이유 " * 20, run_id=None)
        )
        store.add_global_memory(
            MemoryEntry(
                key=f"k{i}", text=f"전역 {i} " * 20, updated_at=datetime(2026, 1, 1, tzinfo=SEOUL)
            )
        )
    budget = 2_000
    b = store.memory_brief(project["id"], budget_tokens=budget)
    assert b["tokens"] <= budget and b["refs"]
    assert b["tokens"] == len(b["text"]) // 3
    blocks = {blk.splitlines()[0]: blk for blk in b["text"].split("\n## ")}
    sizes = {k.removeprefix("## "): len(v) // 3 for k, v in blocks.items()}
    assert sizes["전역 기억"] <= budget * 25 // 100
    assert sizes["프로젝트 기억"] <= budget * 50 // 100
    assert sizes["실패한 시도"] <= budget * 25 // 100
    assert sizes["프로젝트 기억"] > sizes["전역 기억"]  # 50% really is the bigger share
    assert any(line.startswith("[lrn_") for line in b["text"].splitlines())
    assert any(r.startswith("fail_") for r in b["refs"]) and any(
        r.startswith("mem_") for r in b["refs"]
    )


def test_memory_brief_failures_and_global(store, project):
    store.add_global_memory(
        MemoryEntry(key="tone", text="존댓말", updated_at=datetime(2026, 1, 1, tzinfo=SEOUL))
    )
    store.record_failure(project["id"], FailedAttempt(what="로그인 우회", why="캡차", run_id=None))
    b = store.memory_brief(project["id"], budget_tokens=2_000)
    assert "존댓말" in b["text"] and "캡차" in b["text"] and b["refs"] == []


def test_memory_brief_tiny_budget(store, project):
    for i in range(30):
        store.record_failure(
            project["id"], FailedAttempt(what=f"시도 {i}" * 30, why="x" * 50, run_id=None)
        )
    b = store.memory_brief(project["id"], budget_tokens=100)
    assert b["tokens"] <= 100


def test_backlog_and_kr_and_events(store, project, log):
    bid = store.add_backlog(project["id"], BacklogItem(title="a", intent="i", done_criteria=["d"]))
    store.update_backlog(bid, status="dropped", note="중복")
    assert store.get(project["id"])["backlog"][0]["status"] == "dropped"
    store.update_kr(project["id"], "kr_1", value=3.0, evidence=ev())
    assert store.get(project["id"])["goal"]["key_results"][0]["current"] == 3.0
    store.record_failure(project["id"], FailedAttempt(what="w", why="y", run_id="run_1"))
    store.attach_artifacts(
        project["id"],
        [
            {
                "id": "art_1",
                "title": "t",
                "path": "p",
                "status": "partial",
                "evidence": [],
                "verifier_run_id": None,
            }
        ],
    )
    p = store.get(project["id"])
    assert p["memory"]["failed_attempts"][0]["run_id"] == "run_1"
    assert p["artifacts"][0]["id"] == "art_1"
    assert {
        "project.backlog_added",
        "project.backlog_updated",
        "project.kr_updated",
        "project.failure_recorded",
    } <= set(kinds(log))


def test_learning_roundtrip_with_evidence(store, project):
    store.record_learning(project["id"], Learning(text="L", evidence=[ev()]))
    got = store.get(project["id"])["memory"]["learnings"][0]
    assert got["evidence"][0]["captured_at"] == ev()["captured_at"]


def test_goal_file_edit_applies_directly(store, project, home):
    goal_md = home / "projects" / project["slug"] / "GOAL.md"
    goal_md.write_text(goal_md.read_text().replace("고객보다 먼저", "고객보다 하루 먼저"))
    evs = store.ingest_file_edits()
    assert "하루 먼저" in store.get(project["id"])["goal"]["objective"]
    assert any(e["kind"] == "project.goal_edited" and e["payload"]["by"] == "file" for e in evs)
    assert store.ingest_file_edits() == []  # idempotent


def test_goal_file_kr_edit(store, project, home):
    goal_md = home / "projects" / project["slug"] / "GOAL.md"
    goal_md.write_text(goal_md.read_text().replace("| 5.0 |", "| 8.0 |"))
    store.ingest_file_edits()
    assert store.get(project["id"])["goal"]["key_results"][0]["target"] == 8.0


def test_policy_line_removed_in_file_deactivates(store, project, home):
    keep = store.add_policy(project["id"], Policy(text="keep", source_ask=None))
    gone = store.add_policy(project["id"], Policy(text="gone", source_ask=None))
    f = home / "projects" / project["slug"] / "POLICIES.md"
    f.write_text("\n".join(x for x in f.read_text().splitlines() if f"[{gone}]" not in x) + "\n")
    evs = store.ingest_file_edits()
    act = {x["id"]: x["active"] for x in store.get(project["id"])["policies"]}
    assert act == {keep: True, gone: False}
    assert any(
        e["kind"] == "project.policy_deactivated" and e["payload"]["by"] == "file" for e in evs
    )
    assert f"[{gone}]" not in f.read_text()


def test_policy_text_edit_rejected(store, project, home):
    pid = store.add_policy(project["id"], Policy(text="orig", source_ask=None))
    f = home / "projects" / project["slug"] / "POLICIES.md"
    f.write_text(f"- [{pid}] changed\n")
    evs = store.ingest_file_edits()
    assert any(e["kind"] == "project.file_edit_rejected" for e in evs)
    assert store.get(project["id"])["policies"][0]["active"] is True


def test_memory_context_edit_applies(store, project, home):
    f = home / "projects" / project["slug"] / "MEMORY.md"
    f.write_text(f.read_text().replace("C사 중심", "D사 중심"))
    evs = store.ingest_file_edits()
    assert store.get(project["id"])["memory"]["context"] == "D사 중심"
    assert any(e["kind"] == "project.memory_edited" for e in evs)


def test_unparseable_edit_is_not_applied(store, project, home):
    f = home / "projects" / project["slug"] / "GOAL.md"
    f.write_text("<<<깨진 파일")
    evs = store.ingest_file_edits()
    assert any(e["kind"] == "project.file_edit_rejected" for e in evs)
    assert "고객보다" in store.get(project["id"])["goal"]["objective"]
    assert f.read_text() == "<<<깨진 파일"  # human text not clobbered
    assert store.ingest_file_edits() == []  # rejected once, not every tick


def test_pending_human_edit_survives_state_change(store, project, home):
    f = home / "projects" / project["slug"] / "GOAL.md"
    f.write_text(f.read_text().replace("먼저", "일찍"))
    store.add_policy(project["id"], Policy(text="p", source_ask=None))  # triggers render
    store.ingest_file_edits()
    assert "일찍" in store.get(project["id"])["goal"]["objective"]


def test_duplicate_slug_rejected(store, project):
    with pytest.raises(sqlite3.IntegrityError):
        store.create(make_project(slug="compet", channel="telegram:-1:2"))


def test_human_goal_edit_then_update_kr_before_ingest(store, project, home):
    f = home / "projects" / project["slug"] / "GOAL.md"
    f.write_text(f.read_text().replace("먼저", "일찍"))
    store.update_kr(project["id"], "kr_1", value=3.0, evidence=ev())
    store.ingest_file_edits()
    g = store.get(project["id"])["goal"]
    assert "일찍" in g["objective"] and g["key_results"][0]["current"] == 3.0
    assert "| 3.0" in f.read_text() and "일찍" in f.read_text()


def test_human_policy_delete_then_add_policy_before_ingest(store, project, home):
    a = store.add_policy(project["id"], Policy(text="A", source_ask=None))
    f = home / "projects" / project["slug"] / "POLICIES.md"
    f.write_text("")
    b = store.add_policy(project["id"], Policy(text="B", source_ask=None))
    store.ingest_file_edits()
    act = {x["id"]: x["active"] for x in store.get(project["id"])["policies"]}
    assert act == {a: False, b: True}
    assert f"[{b}] B" in f.read_text() and f"[{a}]" not in f.read_text()


def test_memory_lists_edit_rejected_context_applied(store, project, home):
    store.record_learning(project["id"], Learning(text="원래", evidence=[]))
    f = home / "projects" / project["slug"] / "MEMORY.md"
    assert "(읽기 전용)" in f.read_text()
    f.write_text(f.read_text().replace("원래", "조작").replace("C사 중심", "E사 중심"))
    evs = store.ingest_file_edits()
    p = store.get(project["id"])
    assert p["memory"]["context"] == "E사 중심"
    assert p["memory"]["learnings"][0]["text"] == "원래"
    assert any(
        e["kind"] == "project.file_edit_rejected" and "append-only" in e["payload"]["error"]
        for e in evs
    )
    assert "조작" not in f.read_text() and "E사 중심" in f.read_text()


def gfile(home):
    return home / "MEMORY.md"


def gentry(k="k", t="전역"):
    return MemoryEntry(key=k, text=t, updated_at=datetime(2026, 1, 1, tzinfo=SEOUL))


def test_global_memory_rendered_with_ids(store, home):
    store.add_global_memory(gentry("tone", "존댓말"))
    line = [x for x in gfile(home).read_text().splitlines() if x.startswith("- ")][0]
    assert line.startswith("- [mem_") and line.endswith("] 존댓말")


def test_global_file_add_edit_remove(store, home, log, db):
    store.add_global_memory(gentry("a", "첫째"))
    store.add_global_memory(gentry("b", "둘째"))
    f = gfile(home)
    lines = f.read_text().splitlines()
    ida = next(x for x in lines if "첫째" in x).split("]")[0][3:]
    edited = [x for x in lines if "둘째" not in x]  # remove b
    edited = [x.replace("첫째", "첫째 수정") for x in edited] + ["- 셋째 새 항목"]
    f.write_text("\n".join(edited) + "\n")
    evs = store.ingest_file_edits()
    kinds_ = {e["kind"] for e in evs}
    assert {"memory.entry_added", "memory.entry_updated", "memory.entry_deactivated"} <= kinds_
    txt = f.read_text()
    assert "첫째 수정" in txt and "셋째 새 항목" in txt and "둘째" not in txt and ida in txt
    rows = db.execute("SELECT text, active FROM global_memory ORDER BY text").fetchall()
    assert any(r["text"] == "둘째" and r["active"] == 0 for r in rows)  # row kept
    assert store.ingest_file_edits() == []


def test_global_file_unparseable_rejected(store, home):
    store.add_global_memory(gentry())
    f = gfile(home)
    f.write_text("<<<깨짐")
    evs = store.ingest_file_edits()
    assert [e["kind"] for e in evs] == ["memory.file_edit_rejected"]
    assert f.read_text() == "<<<깨짐"
    assert store.ingest_file_edits() == []


def test_global_edit_survives_concurrent_machine_add(store, home):
    store.add_global_memory(gentry("a", "첫째"))
    f = gfile(home)
    f.write_text(f.read_text().replace("첫째", "첫째 수정"))
    store.add_global_memory(gentry("b", "둘째"))
    store.ingest_file_edits()
    txt = f.read_text()
    assert "첫째 수정" in txt and "둘째" in txt


def test_create_is_atomic(store):
    bad = make_project(slug="dup")
    bad["goal"]["key_results"] = bad["goal"]["key_results"] * 2  # duplicate KR ids
    with pytest.raises(sqlite3.IntegrityError):
        store.create(bad)
    assert store.list() == [] and store.by_channel(bad["channel"]) is None


def test_set_backlog_priority(store, project, log):
    bid = store.add_backlog(project["id"], BacklogItem(title="a", intent="i", done_criteria=["d"]))
    store.set_backlog_priority(bid, 7)
    assert store.get(project["id"])["backlog"][0]["priority"] == 7
    [e] = [e for e in log.query(kinds=["project.backlog_reprioritized"])]
    assert e["payload"] == {"item_id": bid, "from": 0, "priority": 7}
    with pytest.raises(KeyError):
        store.set_backlog_priority("bl_missing", 1)
