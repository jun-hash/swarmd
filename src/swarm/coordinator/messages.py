"""Brain schemas for the Coordinator's conversation and a message classification helper."""

import re
from collections.abc import Sequence
from datetime import datetime
from typing import Literal, get_args

from pydantic import BaseModel, Field

from swarm.ports import Brain, BrainError
from swarm.types import AskView

Category = Literal[
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
]
CATEGORIES: tuple[str, ...] = get_args(Category)
# a backlog item title made from a message summary (new work) or a triage Candidate
BACKLOG_TITLE_MAX = 80

_CATEGORY_HELP = {
    "new_work": "새로 시킬 일 (조사·작성 등). 기한이 있으면 due에",
    "goal_change": "프로젝트 목표·KR을 바꾸자는 말",
    "strategy_change": "목표는 그대로, 접근 방식을 바꾸자는 말",
    "policy_add": "앞으로의 규칙 (예: '이런 건 묻지 마')",
    "feedback": "결과물·행동에 대한 평가",
    "ask_answer": "열린 질문(#번호)에 대한 답. 번호가 보이면 target_ask_number에",
    "status": "진행 상황을 묻는 말",
    "steer": "실행 중인 작업의 방향을 바꾸는 말. 대상이 보이면 target_run_hint에",
    "takeover": "사람이 브라우저를 직접 넘겨받겠다는 말",
    "release": "사람이 처리를 끝내고 작업을 돌려주는 말 (예: '됐어')",
}


class MessageClass(BaseModel):
    category: Category
    target_ask_number: int | None
    target_run_hint: str | None
    summary: str
    due: str | None  # ISO 8601 local time in the configured zone, e.g. "2026-11-12T18:00"
    confidence: float = Field(ge=0.0, le=1.0)


class Candidate(BaseModel):
    title: str
    intent: str
    value: int = Field(ge=1, le=5)
    reason: str


class Candidates(BaseModel):
    """Discover output -> triage backlog candidates (Brain purpose="value_estimate")."""

    items: list[Candidate]


MIN_ANSWER_CONFIDENCE = 0.7  # below this, an un-numbered answer with 2+ open Asks re-asks

OpenAsk = tuple[int, AskView]  # (topic number "#n", ask)


def _classify_prompt(
    text: str, open_asks: Sequence[OpenAsk], now: datetime, running: Sequence[str]
) -> str:
    lines = [
        "사람이 보낸 메시지를 아래 범주 중 하나로 분류하라.",
        *[f"- {c}: {_CATEGORY_HELP[c]}" for c in CATEGORIES],
        "",
        f"현재 시각: {now.isoformat(timespec='minutes')} ({now.strftime('%A')}).",
        "상대적 기한('금요일까지', '내일 오전')은 현재 시각 기준 ISO 8601로 due에. 없으면 null.",
        "summary는 한 줄 요약. confidence는 0~1.",
        "",
        "열린 질문:",
        *(
            [f"- #{n} {a['question']} (선택지: {' / '.join(a['options'])})" for n, a in open_asks]
            or ["- 없음"]
        ),
        "",
        "실행 중인 작업:",
        *([f"- {r}" for r in running] or ["- 없음"]),
        "",
        "메시지:",
        text,
    ]
    return "\n".join(lines)


async def classify(
    brain: Brain,
    text: str,
    open_asks: Sequence[OpenAsk],
    *,
    now: datetime,
    running: Sequence[str] = (),
    timeout_s: int = 60,
) -> MessageClass:
    """One Brain call; BrainError propagates (the caller decides the interim ack/fallback)."""
    return await brain.decide(
        purpose="classify_message",
        prompt=_classify_prompt(text, open_asks, now, running),
        schema=MessageClass,
        timeout_s=timeout_s,
    )


def resolve_ask_target(
    mc: MessageClass, open_numbers: Sequence[int], *, min_confidence: float = MIN_ANSWER_CONFIDENCE
) -> int | None:
    """The open Ask (topic number) an answer closes.

    Named "#n" -> that Ask if it is open. No number -> the most recent open Ask in the topic;
    with 2+ open Asks only when the classification is confident. None -> re-ask, never guess.
    """
    if mc.target_ask_number is not None:
        return mc.target_ask_number if mc.target_ask_number in open_numbers else None
    if not open_numbers:
        return None
    if len(open_numbers) == 1:
        return open_numbers[0]
    return max(open_numbers) if mc.confidence >= min_confidence else None


class AnswerChoice(BaseModel):
    """Brain purpose="map_answer": which option a free-text answer picks (None = unclear)."""

    choice: str | None
    confidence: float = Field(ge=0.0, le=1.0)


MIN_MAP_CONFIDENCE = 0.8
_ASK_REF = re.compile(r"#\d+")
_TRAIL = re.compile(r"[\s.!~]+$")
# trailing particles of a plain pick: "B로", "B로 가", "A안으로 가요", "B요"
_PARTICLES = re.compile(r"\s*(?:(?:으로|로)\s*(?:가요|가|해요|해|할게요|할게|하자|진행)?|이요|요)$")
# a negated answer never maps deterministically ("B는 별로", "B 안 돼" -> the Brain decides)
_NEGATION = re.compile(
    r"말고|아니|싫|별로|하지\s*마|안\s*(?:돼|되|해|할|함)|(?<![가-힣A-Za-z])안(?![가-힣])"
    r"|\bnot\b|\bno\b",
    re.IGNORECASE,
)


def _bare(text: str) -> str:
    return _TRAIL.sub("", _ASK_REF.sub("", text)).strip().casefold()


def match_option(text: str, options: Sequence[str], *, strict: bool) -> str | None:
    """Deterministic answer -> option, or None (then the Brain maps it; or re-ask).

    Only an exact match of the bare answer ("#n" and trailing punctuation removed) counts.
    Non-strict answers may also drop trailing particles ("B로 가" -> "B"). `strict`
    (approval / irreversible asks) allows no stripping. A negation marker anywhere outside
    the option text means no match.
    """
    bare = _bare(text)
    forms = {bare} if strict else {bare, _PARTICLES.sub("", bare).strip()}
    exact = [o for o in options if o.casefold() in forms]
    if len(exact) != 1:
        return None
    rest = text.casefold().replace(exact[0].casefold(), " ")
    return None if _NEGATION.search(rest) else exact[0]


async def map_answer(brain: Brain, text: str, ask: AskView, *, timeout_s: int = 30) -> str | None:
    """Brain mapping of an answer to one of the Ask's options; None when unsure or failed."""
    prompt = "\n".join(
        [
            "사람의 답이 질문의 선택지 중 무엇을 고른 것인지 판단하라.",
            "분명하지 않으면 choice는 null. confidence는 0~1.",
            f"질문: {ask['question']}",
            "선택지:",
            *[f"- {o}" for o in ask["options"]],
            "",
            f"답: {text}",
        ]
    )
    try:
        r = await brain.decide(
            purpose="map_answer", prompt=prompt, schema=AnswerChoice, timeout_s=timeout_s
        )
    except BrainError:
        return None
    if r.choice in ask["options"] and r.confidence >= MIN_MAP_CONFIDENCE:
        return r.choice
    return None
