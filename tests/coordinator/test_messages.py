from datetime import datetime

import pytest
from fakes import FakeBrain
from pydantic import ValidationError

from swarm.clock import SEOUL
from swarm.coordinator.messages import (
    CATEGORIES,
    MIN_ANSWER_CONFIDENCE,
    Candidates,
    MessageClass,
    classify,
    resolve_ask_target,
)
from swarm.ports import BrainError, Notifier, Outbound
from swarm.types import AskView

NOW = datetime(2026, 11, 7, 10, 0, tzinfo=SEOUL)


def mc(**kw) -> MessageClass:
    base = dict(
        category="new_work",
        target_ask_number=None,
        target_run_hint=None,
        summary="C사 무료 티어 영향",
        due="2026-11-12T18:00",
        confidence=0.9,
    )
    base.update(kw)
    return MessageClass(**base)


def ask_view(ask_id="ask_1", question="어느 안으로 갈까?") -> AskView:
    return AskView(
        ask_id=ask_id,
        project_id="prj_a",
        run_id="run_1",
        kind="decision",
        question=question,
        options=["A안", "B안"],
        recommended="B안",
        irreversible=False,
        deadline=None,
    )


def test_message_class_categories_are_exact():
    assert CATEGORIES == (
        "new_work",
        "goal_change",
        "strategy_change",
        "policy_add",
        "feedback",
        "ask_answer",
        "status",
        "steer",
        "takeover",
        "release",
    )
    assert set(MessageClass.model_json_schema()["properties"]) == {
        "category",
        "target_ask_number",
        "target_run_hint",
        "summary",
        "due",
        "confidence",
    }
    with pytest.raises(ValidationError):
        mc(category="chitchat")
    with pytest.raises(ValidationError):
        mc(confidence=1.5)


def test_candidates_schema():
    c = Candidates.model_validate(
        {"items": [{"title": "가격표 비교", "intent": "C사 가격", "value": 4, "reason": "KR"}]}
    )
    assert c.items[0].value == 4
    with pytest.raises(ValidationError):
        Candidates.model_validate(
            {"items": [{"title": "t", "intent": "i", "value": 9, "reason": ""}]}
        )


async def test_classify_calls_brain_with_classify_purpose_and_schema():
    brain = FakeBrain(mc())
    r = await classify(
        brain,
        "C사 무료 티어 영향 금요일까지",
        [(2, ask_view("ask_2", "로그인 계정은?"))],
        now=NOW,
        running=["run_9: C사 가격 조사"],
    )
    assert r.category == "new_work"
    call = brain.calls[0]
    assert call["purpose"] == "classify_message" and call["schema"] is MessageClass
    p = call["prompt"]
    assert "C사 무료 티어 영향 금요일까지" in p
    assert "#2" in p and "로그인 계정은?" in p
    assert "run_9: C사 가격 조사" in p
    assert "2026-11-07" in p  # relative dues ("금요일까지") resolve against now
    for cat in CATEGORIES:
        assert cat in p


async def test_classify_propagates_brain_error():
    brain = FakeBrain(error=BrainError("engine unavailable"))
    with pytest.raises(BrainError):
        await classify(brain, "안녕", [], now=NOW)


async def test_classify_passes_timeout():
    brain = FakeBrain(mc(category="status", due=None))
    await classify(brain, "어때?", [], now=NOW, timeout_s=8)
    assert brain.calls[0]["timeout_s"] == 8


def test_resolve_ask_target():
    assert resolve_ask_target(mc(category="ask_answer", target_ask_number=2), [1, 2]) == 2
    assert resolve_ask_target(mc(category="ask_answer", target_ask_number=7), [1, 2]) is None
    assert resolve_ask_target(mc(category="ask_answer"), [3]) == 3  # the only open one
    assert resolve_ask_target(mc(category="ask_answer", confidence=0.2), [3]) == 3
    assert resolve_ask_target(mc(category="ask_answer"), []) is None


def test_unnumbered_answer_with_two_open_asks_goes_to_most_recent_when_confident():
    assert MIN_ANSWER_CONFIDENCE == 0.7
    assert resolve_ask_target(mc(category="ask_answer", confidence=0.9), [1, 4, 2]) == 4
    assert resolve_ask_target(mc(category="ask_answer", confidence=0.7), [1, 2]) == 2


def test_unnumbered_answer_with_two_open_asks_and_low_confidence_reasks():
    assert resolve_ask_target(mc(category="ask_answer", confidence=0.5), [1, 2]) is None
    assert (
        resolve_ask_target(mc(category="ask_answer", confidence=0.5), [1, 2], min_confidence=0.4)
        == 2
    )


async def test_notifier_port_shape():
    sent: list[Outbound] = []

    class Spy:
        async def send(self, msg: Outbound) -> None:
            sent.append(msg)

    n: Notifier = Spy()
    msg = Outbound(
        channel="telegram:-100123:42",
        kind="ask",
        text="🙋 결정 · C사 대응안 추천 B",
        ref_id="ask_1",
        urgent=False,
    )
    assert await n.send(msg) is None
    assert sent == [msg]
    assert set(Outbound.__annotations__) == {"channel", "kind", "text", "ref_id", "urgent"}


def test_match_option_exact_and_unique_substring():
    from swarm.coordinator.messages import match_option

    assert match_option("#2 B로 가", ["A", "B"], strict=False) == "B"
    assert match_option("#2 A안 B안 다 별로", ["A안", "B안"], strict=False) is None  # 2 hits
    assert match_option("#1 승인", ["승인", "거절"], strict=True) == "승인"
    # approval-like asks take exact options only: "승인 안 해" must never approve
    assert match_option("#1 승인 안 해", ["승인", "거절"], strict=True) is None
    assert match_option("자유 답", [], strict=False) is None


async def test_map_answer_uses_map_answer_purpose_and_rejects_unknown_choice():
    from swarm.coordinator.messages import AnswerChoice, map_answer

    brain = FakeBrain(AnswerChoice(choice="B안", confidence=0.9))
    assert await map_answer(brain, "두 번째로", ask_view()) == "B안"
    assert brain.calls[0]["purpose"] == "map_answer"
    assert "두 번째로" in brain.calls[0]["prompt"] and "A안" in brain.calls[0]["prompt"]
    assert (
        await map_answer(FakeBrain(AnswerChoice(choice="C안", confidence=0.9)), "C", ask_view())
        is None
    )
    assert (
        await map_answer(FakeBrain(AnswerChoice(choice="A안", confidence=0.3)), "음", ask_view())
        is None
    )
    assert await map_answer(FakeBrain(error=BrainError("x")), "음", ask_view()) is None


@pytest.mark.parametrize(
    "text",
    ["B는 별로", "#2 B 안 돼", "B 말고", "B 아니야", "B 싫어", "not B", "no B", "B로 하지 마"],
)
def test_match_option_never_maps_a_negated_answer(text):
    from swarm.coordinator.messages import match_option

    assert match_option(text, ["A", "B"], strict=False) is None


@pytest.mark.parametrize(
    ("text", "want"),
    [("#2 B로 가", "B"), ("B로", "B"), ("B요", "B"), ("A안으로 가요", "A안"), ("#1 B안", "B안")],
)
def test_match_option_strips_particles_for_an_exact_match(text, want):
    from swarm.coordinator.messages import match_option

    assert match_option(text, ["A", "B", "A안", "B안"], strict=False) == want


def test_match_option_needs_an_exact_bare_match():
    from swarm.coordinator.messages import match_option

    assert match_option("B가 좋겠어 근데 A도", ["A", "B"], strict=False) is None  # Brain maps
    assert match_option("B 쪽으로 가자", ["A", "B"], strict=False) is None
