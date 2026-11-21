"""ContextPolicy: the compaction-experiment slot of the own harness.

A policy decides (a) what the first prompt of a fresh engine session contains, (b) whether the
next engine turn resumes the current session or starts a new one, (c) how progress is carried
between sessions, and (d) how observations are compressed. The harness only executes these
decisions; it never decides on its own when to cut a session.

Guarantees: `compose` <= 40% of the engine context (token estimate), observations <= 8,000
chars. Policies are pure: `after_turn` returns a new Carry and never mutates its input.

Observation knob: the browser MCP server (a separate process) renders observations itself,
so the knob that actually takes effect is `observe` ("naive" -> observe.truncate_ax_tree,
"compressed" -> observe.compress_ax_tree, see observe.OBSERVERS; passed as `--policy`).
`compress_observation` is the policy-side hook reserved for compression done inside the harness
(e.g. text the harness itself injects); the own harness does not call it.
"""

import re
from dataclasses import dataclass, field, replace
from typing import Literal, Protocol

from swarm.engine.base import TurnUsage
from swarm.harness.own.prompts import render_worker_prompt
from swarm.types import HarnessTask, Observation

CONTEXT_TOKENS = 200_000  # engine context window (claude)
COMPOSE_MAX_FRAC = 0.4
# Conservative char budget: est_tokens() never counts more than one token per char, so a prompt
# of at most this many chars is always within COMPOSE_MAX_FRAC of the context.
COMPOSE_MAX_CHARS = int(CONTEXT_TOKENS * COMPOSE_MAX_FRAC)
OBSERVATION_MAX_CHARS = 8_000
# Context growth per assistant turn when no measured usage is available: one compressed
# observation (<= 8,000 chars ~ 2,700 tokens) plus the call and reasoning around it.
EST_TOKENS_PER_TURN = 3_000
NOTE_MAX_CHARS = 600
NOTES_KEEP = 30

ToolResultText = str
ObserveMode = Literal["naive", "compressed"]  # passed to swarm-browser-mcp --policy


def est_tokens(text: str) -> int:
    """Rough token estimate: ASCII ~3 chars/token, anything else (Hangul etc.) 1 token/char."""
    ascii_n = sum(1 for ch in text if ch < "\x80")
    return ascii_n // 3 + (len(text) - ascii_n)


@dataclass(frozen=True)
class Carry:
    """What survives between engine turns (and, for rollover policies, between sessions)."""

    engine_session_id: str | None
    turns_in_session: int
    notes: list[str] = field(default_factory=list)
    open_tabs: list[str] = field(default_factory=list)
    est_context_frac: float = 0.0

    @classmethod
    def empty(cls) -> "Carry":
        return cls(engine_session_id=None, turns_in_session=0)


@dataclass(frozen=True)
class TurnSummary:
    turns: int  # assistant turns (tool calls) since the previous summary
    usage: TurnUsage | None
    last_text: str


class ContextPolicy(Protocol):
    id: str  # "naive_resume" | "rollover_brief" | "inline_spill" ...
    observe: ObserveMode  # extension: THE observation knob (browser MCP --policy)

    def compose(self, task: HarnessTask, *, carry: Carry) -> str: ...  # first prompt of a session

    def should_resume(self, carry: Carry) -> bool: ...  # continue the current engine session?

    def after_turn(self, carry: Carry, turn: TurnSummary) -> Carry: ...  # notes / refs update

    # reserved for in-harness compression; not called by the own harness (see module doc)
    def compress_observation(self, obs: Observation) -> ToolResultText: ...

    # extension: why should_resume() said False (for the ctx.rollover event); None = it did not
    def rollover_reason(self, carry: Carry) -> str | None: ...


def _observation_header(obs: Observation) -> str:
    return f"url: {obs['url']}\ntitle: {obs['title']}\n---\n"


def _cap(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    marker = f"\n…(잘림, 전체 {len(text)}자)"
    return text[: limit - len(marker)] + marker


def _measured_frac(usage: TurnUsage | None) -> float:
    if usage is None:
        return 0.0
    used = usage.input_tokens + usage.cached_tokens + usage.output_tokens
    return used / CONTEXT_TOKENS


def _grow(carry: Carry, turn: TurnSummary) -> Carry:
    est = carry.est_context_frac + turn.turns * EST_TOKENS_PER_TURN / CONTEXT_TOKENS
    return replace(
        carry,
        turns_in_session=carry.turns_in_session + turn.turns,
        est_context_frac=max(est, _measured_frac(turn.usage)),
    )


class NaiveResume:
    """Baseline: always resume the same engine session; compaction is left to the engine.
    Observations are the raw accessibility tree, truncated."""

    id = "naive_resume"
    observe: ObserveMode = "naive"

    def compose(self, task: HarnessTask, *, carry: Carry) -> str:
        # no progress notes (baseline); open tabs are plain facts, needed when a lost session
        # is restarted so the worker continues in place instead of reopening start_url
        return render_worker_prompt(
            task, notes=[], open_tabs=carry.open_tabs, max_chars=COMPOSE_MAX_CHARS
        )

    def should_resume(self, carry: Carry) -> bool:
        return carry.engine_session_id is not None

    def after_turn(self, carry: Carry, turn: TurnSummary) -> Carry:
        return _grow(carry, turn)

    def compress_observation(self, obs: Observation) -> ToolResultText:
        return _cap(_observation_header(obs) + obs["text"], OBSERVATION_MAX_CHARS)

    def rollover_reason(self, carry: Carry) -> str | None:
        return None


_SPACES = re.compile(r"[ \t ]+")


class RolloverBrief:
    """Ralph-style: every `rollover_turns` assistant turns, or when the estimated context use
    reaches `rollover_frac`, start a fresh session primed with intent + done criteria +
    memory_brief + progress notes + open tabs. Observations are ranked/compressed."""

    id = "rollover_brief"
    observe: ObserveMode = "compressed"

    def __init__(self, rollover_turns: int = 40, rollover_frac: float = 0.7) -> None:
        self.rollover_turns = rollover_turns
        self.rollover_frac = rollover_frac

    def compose(self, task: HarnessTask, *, carry: Carry) -> str:
        return render_worker_prompt(
            task, notes=carry.notes, open_tabs=carry.open_tabs, max_chars=COMPOSE_MAX_CHARS
        )

    def should_resume(self, carry: Carry) -> bool:
        return carry.engine_session_id is not None and self.rollover_reason(carry) is None

    def rollover_reason(self, carry: Carry) -> str | None:
        if carry.turns_in_session >= self.rollover_turns:
            return "turns"
        if carry.est_context_frac >= self.rollover_frac:
            return "context"
        return None

    def after_turn(self, carry: Carry, turn: TurnSummary) -> Carry:
        grown = _grow(carry, turn)
        note = " ".join(turn.last_text.split())
        notes = list(carry.notes)
        if note and (not notes or notes[-1] != _cap_note(note)):
            notes.append(_cap_note(note))
        return replace(grown, notes=notes[-NOTES_KEEP:])

    def compress_observation(self, obs: Observation) -> ToolResultText:
        lines: list[str] = []
        for raw in obs["text"].splitlines():
            line = _SPACES.sub(" ", raw).strip()
            if line and (not lines or lines[-1] != line):
                lines.append(line)
        return _cap(_observation_header(obs) + "\n".join(lines), OBSERVATION_MAX_CHARS)


def _cap_note(note: str) -> str:
    return note if len(note) <= NOTE_MAX_CHARS else note[: NOTE_MAX_CHARS - 1] + "…"


POLICIES: dict[str, type] = {NaiveResume.id: NaiveResume, RolloverBrief.id: RolloverBrief}


def get_policy(policy_id: str) -> ContextPolicy:
    """Default-configured policy by id (Variant.context_policy). KeyError if unknown."""
    return POLICIES[policy_id]()
