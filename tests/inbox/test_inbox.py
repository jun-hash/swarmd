import asyncio
from datetime import timedelta

import pytest

from swarm.inbox import inbox as inbox_mod
from swarm.inbox.inbox import AskAlreadyClosed, PolicyMatch

from .conftest import draft


def kinds(log):
    return [e["kind"] for e in log.query()]


async def test_policy_match_answers_synchronously(inbox, fake_brain, with_policy, log):
    fake_brain.returns(PolicyMatch(policy_id=with_policy, choice="회사 계정 사용", confidence=0.9))
    t = await inbox.raise_ask("run_1", draft(kind="takeover", irreversible=False))
    assert t["outcome"]["status"] == "answered" and t["outcome"]["via"] == "policy"
    assert "ask.policy_matched" in kinds(log)
    assert inbox.open_asks() == []
    assert inbox.drain_closed() == []  # delivered in the ticket; never via drain


async def test_prompt_has_question_and_only_active_policies(
    inbox, fake_brain, projects, pid, with_policy
):
    projects.add_policy(pid, {"text": "폐기된 정책XYZ", "source_ask": None})
    dead = [p for p in projects.get(pid)["policies"] if "XYZ" in p["text"]][0]["id"]
    projects.deactivate_policy(dead, reason="x")
    await inbox.raise_ask("run_1", draft(question="질문QQQ"))
    p = fake_brain.prompts[0]
    assert "질문QQQ" in p and "회사 계정 사용" in p and "계정은 항상 회사 계정" in p
    assert "XYZ" not in p


async def test_low_confidence_goes_to_human(inbox, fake_brain, with_policy):
    fake_brain.returns(PolicyMatch(policy_id=with_policy, choice="회사 계정 사용", confidence=0.7))
    t = await inbox.raise_ask("run_1", draft())
    assert t["outcome"] is None and len(inbox.open_asks()) == 1


async def test_unknown_policy_id_ignored(inbox, fake_brain, with_policy):
    fake_brain.returns(PolicyMatch(policy_id="pol_nope", choice="x", confidence=0.99))
    assert (await inbox.raise_ask("run_1", draft()))["outcome"] is None


async def test_no_active_policies_skips_brain(inbox, fake_brain):
    t = await inbox.raise_ask("run_1", draft())
    assert t["outcome"] is None and fake_brain.calls == 0


async def test_brain_error_goes_to_human(inbox, fake_brain, with_policy):
    t = await inbox.raise_ask("run_1", draft())  # FakeBrain raises BrainError
    assert fake_brain.calls == 1 and t["outcome"] is None


async def test_brain_timeout_goes_to_human(inbox, fake_brain, with_policy, monkeypatch):
    monkeypatch.setattr(inbox_mod, "POLICY_TIMEOUT_S", 0.05)
    fake_brain.delay = 1
    fake_brain.returns(PolicyMatch(policy_id=with_policy, choice="a", confidence=1))
    t = await asyncio.wait_for(inbox.raise_ask("run_1", draft()), 2)
    assert t["outcome"] is None


async def test_irreversible_skips_policy_and_has_no_default(inbox, fake_brain, with_policy, clock):
    t = await inbox.raise_ask(
        "run_1", draft(kind="approval", irreversible=True, default_after=timedelta(hours=1))
    )
    assert t["outcome"] is None and fake_brain.calls == 0
    assert inbox.open_asks()[0]["default_after"] is None
    inbox.mark_sent(t["ask_id"], clock.now())
    clock.advance(timedelta(days=3))
    assert await inbox.tick(clock.now()) == []


async def test_default_counts_from_sent_time_not_creation(inbox, clock):
    t = await inbox.raise_ask("run_1", draft(default_after=timedelta(hours=1)))
    assert inbox.open_asks()[0]["deadline"] is None
    clock.advance(timedelta(hours=2))
    assert await inbox.tick(clock.now()) == []
    inbox.mark_sent(t["ask_id"], clock.now())
    clock.advance(timedelta(minutes=59))
    assert await inbox.tick(clock.now()) == []
    clock.advance(timedelta(minutes=2))
    out = await inbox.tick(clock.now())
    assert out[0]["status"] == "defaulted" and out[0]["choice"] == "회사 계정 사용"
    assert out[0]["via"] == "default"
    assert await inbox.tick(clock.now()) == []
    drained = inbox.drain_closed()  # tick result is informational; drain delivers exactly once
    assert [o["ask_id"] for o in drained] == [out[0]["ask_id"]]
    assert inbox.drain_closed() == []


async def test_closes_exactly_once_and_drains_once(inbox):
    t = await inbox.raise_ask("run_1", draft())
    await inbox.answer(t["ask_id"], choice="B", raw_text="B로", via="telegram:-1:2")
    with pytest.raises(AskAlreadyClosed):
        await inbox.answer(t["ask_id"], choice="A", raw_text="A", via="x")
    assert len(inbox.drain_closed()) == 1 and inbox.drain_closed() == []


async def test_answer_vs_tick_race_closes_once(inbox, clock):
    t = await inbox.raise_ask("run_1", draft(default_after=timedelta(minutes=1)))
    inbox.mark_sent(t["ask_id"], clock.now())
    clock.advance(timedelta(minutes=5))
    await inbox.answer(t["ask_id"], choice="B", raw_text="B", via="telegram:1")
    assert await inbox.tick(clock.now()) == []
    assert [o["status"] for o in inbox.drain_closed()] == ["answered"]


async def test_topic_numbers_increment_per_channel(inbox):
    a = await inbox.raise_ask("run_1", draft())
    b = await inbox.raise_ask("run_2", draft())
    c = await inbox.raise_ask("run_free", draft())
    assert (inbox.topic_number(a["ask_id"]), inbox.topic_number(b["ask_id"])) == (1, 2)
    assert inbox.topic_number(c["ask_id"]) == 1  # general channel numbers separately


async def test_open_asks_filter_by_project(inbox, pid):
    await inbox.raise_ask("run_1", draft())
    await inbox.raise_ask("run_free", draft())
    assert len(inbox.open_asks()) == 2
    assert [a["project_id"] for a in inbox.open_asks(pid)] == [pid]


async def test_expire_for_run(inbox, log):
    t = await inbox.raise_ask("run_1", draft())
    other = await inbox.raise_ask("run_2", draft())
    out = inbox.expire_for_run("run_1")
    assert [o["status"] for o in out] == ["expired"] and out[0]["ask_id"] == t["ask_id"]
    assert inbox.drain_closed() == []
    assert [a["ask_id"] for a in inbox.open_asks()] == [other["ask_id"]]
    with pytest.raises(AskAlreadyClosed):
        await inbox.answer(t["ask_id"], choice="A", raw_text="A", via="x")
    assert inbox.expire_for_run("run_1") == []
    assert "ask.expired" in kinds(log)


async def test_drain_survives_reopen(inbox, env, fake_brain, clock, run_map):
    t = await inbox.raise_ask("run_1", draft())
    await inbox.answer(t["ask_id"], choice="B", raw_text="B", via="x")
    db, log, projects, _ = env
    again = inbox_mod.SqliteInbox(
        db, log, projects, fake_brain, clock, run_project=run_map.get, general_channel="g"
    )
    assert len(again.drain_closed()) == 1 and inbox.drain_closed() == []


async def test_events_emitted(inbox, log, clock):
    t = await inbox.raise_ask("run_1", draft(default_after=timedelta(minutes=1)))
    inbox.mark_sent(t["ask_id"], clock.now())
    await inbox.answer(t["ask_id"], choice="B", raw_text="B", via="x")
    assert {"ask.raised", "ask.sent", "ask.answered"} <= set(kinds(log))


async def test_choice_not_in_options_goes_to_human(inbox, fake_brain, with_policy):
    fake_brain.returns(PolicyMatch(policy_id=with_policy, choice="없는 선택지", confidence=0.9))
    t = await inbox.raise_ask("run_1", draft())
    assert t["outcome"] is None and len(inbox.open_asks()) == 1


async def test_brain_failure_emits_event(inbox, fake_brain, with_policy, log):
    await inbox.raise_ask("run_1", draft())
    assert "ask.policy_match_failed" in kinds(log)


@pytest.mark.parametrize("rec", [None, "목록에 없음"])
async def test_default_ignored_without_valid_recommended(inbox, clock, log, rec):
    t = await inbox.raise_ask("run_1", draft(recommended=rec, default_after=timedelta(hours=1)))
    assert inbox.open_asks()[0]["default_after"] is None
    assert "ask.default_ignored" in kinds(log)
    inbox.mark_sent(t["ask_id"], clock.now())
    clock.advance(timedelta(days=2))
    assert await inbox.tick(clock.now()) == []
    assert len(inbox.open_asks()) == 1


async def test_zero_default_after_defaults_right_after_send(inbox, clock):
    t = await inbox.raise_ask("run_1", draft(default_after=timedelta(0)))
    assert await inbox.tick(clock.now()) == []  # unsent
    inbox.mark_sent(t["ask_id"], clock.now())
    out = await inbox.tick(clock.now())
    assert out[0]["status"] == "defaulted"
