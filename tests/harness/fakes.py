"""Fakes for harness contract tests: sink, CDP session, task, multi-script engine."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from swarm.clock import SEOUL, FakeClock
from swarm.engine.base import (
    Done,
    EngineEvent,
    EngineTurn,
    Thinking,
    ToolCall,
    ToolResult,
    TurnUsage,
)
from swarm.engine.fake import FakeEngine, FakeRun
from swarm.types import AskDraft, AskOutcome, AskTicket, CdpSession, HarnessTask

GOTO = "mcp__swarmbrowser__browser_goto"
SNAPSHOT = "mcp__swarmbrowser__browser_snapshot"
ASK = "mcp__swarmbrowser__ask_human"
ASK_SENT = "질문을 사람에게 보냈습니다(#7). 지금 턴을 끝내세요. 답은 다음 입력으로 옵니다."

SCRIPT_OK: list[EngineEvent] = [
    Thinking(text="let me look"),
    ToolCall(id="t1", name=GOTO, args={"url": "http://site.test/"}),
    ToolResult(id="t1", ok=True, preview="url: http://site.test/\ntitle: Site", ms=5),
    Thinking(text="The title is Site."),
    TurnUsage(input_tokens=10, output_tokens=5, cached_tokens=0, cost_usd=0.0),
    Done(summary="The title is Site."),
]

SCRIPT_ASK: list[EngineEvent] = [
    ToolCall(
        id="a1",
        name=ASK,
        args={"kind": "decision", "question": "A or B?", "options": ["A", "B"]},
    ),
    ToolResult(id="a1", ok=True, preview=ASK_SENT, ms=3),
    Thinking(text="waiting for the human"),
    Done(summary="asked the human"),
]


def tool_steps(n: int, *, start: int = 0) -> list[EngineEvent]:
    out: list[EngineEvent] = []
    for i in range(start, start + n):
        out.append(ToolCall(id=f"t{i}", name=SNAPSHOT, args={}))
        out.append(ToolResult(id=f"t{i}", ok=True, preview=f"url: http://site.test/{i}", ms=1))
        out.append(Thinking(text=f"step {i} done"))
    return out


def clock() -> FakeClock:
    """A fresh clock a test can advance on its own; 09:00 like the harness conftest's."""
    return FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=SEOUL))


class SeqEngine(FakeEngine):
    """FakeEngine that plays a different script on each start (last one repeats).
    `on_start(turn)` runs before each run is handed out (e.g. to advance a FakeClock)."""

    def __init__(
        self,
        scripts: list[list[EngineEvent]],
        *,
        session_ids: list[str | None] | None = None,
        on_start: Callable[[EngineTurn], None] | None = None,
    ) -> None:
        super().__init__(scripts[0])
        self.scripts = scripts
        self.session_ids = session_ids
        self.on_start = on_start

    async def start(self, turn: EngineTurn) -> FakeRun:
        i = len(self.turns)
        if self.on_start:
            self.on_start(turn)
        self.turns.append(turn)
        script = self.scripts[min(i, len(self.scripts) - 1)]
        sid = self.session_ids[min(i, len(self.session_ids) - 1)] if self.session_ids else None
        run = FakeRun(list(script), sid or turn["resume_id"] or f"sess-{i + 1}")
        self.runs.append(run)
        return run


class FakeSink:
    def __init__(
        self, output_dir: Path, *, outcome: AskOutcome | None = None, run_id: str = "run_1"
    ) -> None:
        self.run_id = run_id
        self.output_dir = output_dir
        self.outcome = outcome
        self.asks: list[AskDraft] = []
        self.records: list[object] = []

    async def raise_ask(self, draft: AskDraft) -> AskTicket:
        self.asks.append(draft)
        return AskTicket(ask_id=f"ask_{len(self.asks)}", outcome=self.outcome)

    def record(self, ev) -> None:
        self.records.append(ev)


def fake_session(**kw) -> CdpSession:
    s = CdpSession(
        id="sess_1",
        cdp_url="ws://127.0.0.1:9/devtools/browser/fake",
        target_id="TARGET1",
        profile="test",
        capabilities=frozenset({"screenshot", "a11y_tree"}),
        guard="guarded",
    )
    s.update(kw)  # type: ignore[typeddict-item]
    return s


def task(**kw) -> HarnessTask:
    t = HarnessTask(
        instruction=(
            "경쟁사 A의 요금제 페이지에서 가격을 확인한다. 이유: 가격 비교표. 제약: 로그인 금지."
        ),
        done_criteria=["플랜별 월 가격", "출처 URL"],
        start_url="http://site.test/",
        memory_brief="A사는 2026-10에 가격을 올렸다.",
        policies=["결제 페이지는 열지 않는다"],
        budget={"max_minutes": 60},
        action_mode="guarded",
    )
    t.update(kw)  # type: ignore[typeddict-item]
    return t
