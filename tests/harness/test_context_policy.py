"""ContextPolicy unit tests: compose size bound, rollover rules, observation cap."""

import pytest

from swarm.engine.base import TurnUsage
from swarm.harness.own import prompts
from swarm.harness.own.context import (
    CONTEXT_TOKENS,
    POLICIES,
    Carry,
    NaiveResume,
    RolloverBrief,
    TurnSummary,
    est_tokens,
    get_policy,
)
from swarm.types import Observation

from .fakes import task


def empty_carry() -> Carry:
    return Carry.empty()


def carry(**kw) -> Carry:
    base = dict(
        engine_session_id="s1", turns_in_session=0, notes=[], open_tabs=[], est_context_frac=0.0
    )
    base.update(kw)
    return Carry(**base)


def test_policy_ids_and_lookup():
    assert NaiveResume().id == "naive_resume" and RolloverBrief().id == "rollover_brief"
    assert set(POLICIES) == {"naive_resume", "rollover_brief"}
    assert isinstance(get_policy("rollover_brief"), RolloverBrief)
    assert NaiveResume().observe == "naive" and RolloverBrief().observe == "compressed"
    with pytest.raises(KeyError):
        get_policy("inline_spill")


def test_rollover_after_40_turns():
    p = RolloverBrief()
    c = Carry(
        engine_session_id="s1", turns_in_session=40, notes=[], open_tabs=[], est_context_frac=0.2
    )
    assert p.should_resume(c) is False
    assert p.should_resume(carry(turns_in_session=39, est_context_frac=0.2)) is True


def test_rollover_at_70pct_context():
    p = RolloverBrief()
    assert p.should_resume(carry(turns_in_session=3, est_context_frac=0.7)) is False
    assert p.should_resume(carry(turns_in_session=3, est_context_frac=0.69)) is True


def test_rollover_reason():
    p = RolloverBrief()
    assert p.rollover_reason(carry(turns_in_session=40)) == "turns"
    assert p.rollover_reason(carry(est_context_frac=0.9)) == "context"
    assert p.rollover_reason(carry()) is None


def test_nothing_to_resume_without_session():
    for p in (NaiveResume(), RolloverBrief()):
        assert p.should_resume(empty_carry()) is False


def test_naive_always_resumes():
    p = NaiveResume()
    assert p.should_resume(carry(turns_in_session=500, est_context_frac=0.99)) is True


def test_compose_within_40pct_context():
    out = RolloverBrief().compose(task(memory_brief="x" * 50_000), carry=empty_carry())
    assert est_tokens(out) <= 0.4 * CONTEXT_TOKENS


@pytest.mark.parametrize("policy", [NaiveResume(), RolloverBrief()], ids=lambda p: p.id)
def test_compose_bound_holds_for_huge_inputs(policy):
    huge = task(
        instruction="i" * 300_000,
        memory_brief="m" * 2_000_000,
        policies=["p" * 50_000] * 20,
        recent_failures=["f" * 50_000] * 20,
    )
    c = carry(notes=["n" * 10_000] * 100, open_tabs=["http://t/" + "u" * 5000] * 50)
    out = policy.compose(huge, carry=c)
    assert est_tokens(out) <= 0.4 * CONTEXT_TOKENS
    for rule in prompts.RULES:  # rules survive truncation
        assert rule in out


def test_compose_contains_intent_criteria_brief_policies_rules():
    t = task(recent_failures=["로그인 벽에서 막힘 — 공개 페이지로 우회"])
    c = carry(notes=["요금제 페이지 찾음"], open_tabs=["http://site.test/pricing"])
    out = RolloverBrief().compose(t, carry=c)
    for piece in (
        t["instruction"],
        "플랜별 월 가격",
        "출처 URL",
        t["memory_brief"],
        "결제 페이지는 열지 않는다",
        "로그인 벽에서 막힘",
        "요금제 페이지 찾음",
        "http://site.test/pricing",
        "현재 탭에서 이어서 진행",  # not the first session: no start_url goto
        "되돌릴 수 없는 행동 전에는 ask_human(irreversible=true)",
        "주장마다 record_evidence",
        "새 탭을 열지 말 것 (현재 탭에서만 이동)",
    ):
        assert piece in out, piece


def test_naive_compose_has_no_progress_notes():
    out = NaiveResume().compose(task(), carry=carry(notes=["secret note"]))
    assert "secret note" not in out and "주장마다 record_evidence" in out


def test_dry_run_mode_is_stated():
    out = RolloverBrief().compose(task(action_mode="dry_run"), carry=empty_carry())
    assert "dry_run" in out


def test_after_turn_counts_turns_and_keeps_notes():
    p = RolloverBrief()
    c = p.after_turn(carry(), TurnSummary(turns=3, usage=None, last_text="가격표 발견"))
    assert c.turns_in_session == 3 and c.notes == ["가격표 발견"]
    assert c.est_context_frac > 0
    c2 = p.after_turn(c, TurnSummary(turns=1, usage=None, last_text="가격표 발견"))
    assert c2.notes == ["가격표 발견"]  # no duplicate consecutive notes
    assert c2.turns_in_session == 4 and c2.est_context_frac > c.est_context_frac


def test_after_turn_uses_measured_usage_when_larger():
    p = RolloverBrief()
    usage = TurnUsage(input_tokens=1000, output_tokens=0, cached_tokens=150_000, cost_usd=0)
    c = p.after_turn(carry(), TurnSummary(turns=1, usage=usage, last_text=""))
    assert c.est_context_frac >= 151_000 / CONTEXT_TOKENS
    assert p.should_resume(c) is False


def test_after_turn_note_list_is_bounded():
    p = RolloverBrief()
    c = carry()
    for i in range(200):
        c = p.after_turn(c, TurnSummary(turns=0, usage=None, last_text=f"note {i} " + "z" * 2000))
    assert len(c.notes) <= 30 and all(len(n) <= 600 for n in c.notes)
    assert c.notes[-1].startswith("note 199")


def test_after_turn_is_pure():
    p = RolloverBrief()
    c = carry()
    p.after_turn(c, TurnSummary(turns=5, usage=None, last_text="x"))
    assert c.turns_in_session == 0 and c.notes == []


@pytest.mark.parametrize("policy", [NaiveResume(), RolloverBrief()], ids=lambda p: p.id)
def test_compress_observation_le_8000(policy):
    obs = Observation(url="http://a", title="T", text=("line   with   spaces\n" * 2000))
    out = policy.compress_observation(obs)
    assert len(out) <= 8000 and out.startswith("url: http://a\ntitle: T")


def test_compressed_observation_collapses_whitespace_and_repeats():
    obs = Observation(url="u", title="t", text="a   b\n\n\n\nc\nc\nc\nd")
    out = RolloverBrief().compress_observation(obs)
    assert "a b" in out and out.count("\nc") == 1 and "\n\n" not in out


def test_start_url_only_on_first_session():
    first = RolloverBrief().compose(task(), carry=empty_carry())
    assert "먼저 browser_goto로 http://site.test/ 을 연다" in first
    assert "현재 탭에서 이어서 진행" not in first
    for c in (carry(notes=["did x"]), carry(open_tabs=["http://site.test/p"])):
        later = RolloverBrief().compose(task(), carry=c)
        assert "browser_goto로 http://site.test/ 을" not in later
        assert "현재 탭에서 이어서 진행" in later


def test_steer_and_resume_prompts():
    p = prompts.render_steer_prompt("x" * 10_000)
    assert p.startswith("사람 지시: …") and len(p) <= prompts.STEER_MAX_CHARS + 200
    assert prompts.RECHECK in p and prompts.RECHECK in prompts.render_resume_prompt("답: B")


@pytest.mark.parametrize("policy", [NaiveResume(), RolloverBrief()], ids=lambda p: p.id)
def test_observation_actually_used_is_le_8000(policy):
    from swarm.harness.own.observe import OBSERVERS

    from .test_observe import ax_node

    nodes = [ax_node(1, "RootWebArea", "big")]
    nodes += [
        ax_node(i, "link" if i % 3 else "StaticText", f"item {i} " * 20) for i in range(2, 3000)
    ]
    text, _ = OBSERVERS[policy.observe](nodes)
    assert len(text) <= 8000
