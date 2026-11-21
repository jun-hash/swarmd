"""OwnHarness: our own harness — engine CLI + swarm-browser-mcp, steered by a ContextPolicy.

One harness run = one or more engine runs ("segments"). A segment ends when the engine finishes,
or the harness cuts it at a tool boundary (right after a ToolResult) because
- a human steer is pending  -> resume the SAME engine session with "사람 지시: <text>", or
- the policy says not to resume -> record `ctx.rollover`, start a NEW session from `compose()`.
Time budget: the engine enforces `EngineTurn.max_minutes`; we pass the order's remaining minutes
into every segment and never start one with no time left. Quota errors ("quota:*") and every
other engine Error end the run unchanged — no new session after them.
Every terminal is preceded by a `SessionCheckpoint` on the sink (session id, notes, open tabs,
unapplied steers); WorkerPool feeds it back as `HarnessTask.resume` to continue later.
"""

import asyncio
import contextlib
import math
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Literal

from swarm.clock import Clock, SystemClock
from swarm.engine.base import Engine, EngineRun, EngineTurn
from swarm.harness.base import (
    AskRaised,
    ContextRollover,
    Done,
    Error,
    FileOutput,
    HarnessEvent,
    HarnessHandle,
    RunSink,
    SessionCheckpoint,
    Thinking,
    ToolCall,
    ToolResult,
    TurnUsage,
    UnknownHandle,
)
from swarm.harness.own.browser_mcp import ASK_SENT_PREFIX, TOOL_NAMES
from swarm.harness.own.context import (
    CONTEXT_TOKENS,
    Carry,
    ContextPolicy,
    TurnSummary,
    est_tokens,
)
from swarm.harness.own.prompts import render_resume_prompt, render_steer_prompt
from swarm.types import CdpSession, HarnessTask, RunId

MCP_SERVER = "swarmbrowser"
TOOL_PREFIX = f"mcp__{MCP_SERVER}__"
ASK_TOOL = TOOL_PREFIX + "ask_human"
SCREENSHOT_TOOL = TOOL_PREFIX + "browser_screenshot"
SCREENSHOT_SAVED = "스크린샷 저장: "
_URL_LINE = re.compile(r"^url: (\S+)", re.M)
_ASK_NUMBER = re.compile(r"\(#(\d+)\)")


def allowed_tools() -> list[str]:
    return [TOOL_PREFIX + n for n in TOOL_NAMES]


@dataclass
class _Live:
    steers: list[str] = field(default_factory=list)
    carry: Carry = field(default_factory=Carry.empty)


async def _just(ev: HarnessEvent) -> AsyncIterator[HarnessEvent]:
    yield ev


def _seed_carry(task: HarnessTask) -> Carry:
    resume = task.get("resume")
    if resume is None:
        return Carry.empty()
    return Carry(
        engine_session_id=resume["session_id"],
        turns_in_session=0,
        notes=list(resume["notes"]),
        open_tabs=list(resume["open_tabs"]),
    )


@dataclass
class _Segment:
    """Per engine run bookkeeping."""

    calls: dict[str, ToolCall] = field(default_factory=dict)
    calls_since_boundary: int = 0
    last_text: str = ""
    usage: TurnUsage | None = None
    ask: AskRaised | None = None
    terminal: Done | Error | None = None
    cut: Literal["steer", "rollover"] | None = None


class OwnHarness:
    id = "own"
    needs_external_browser = True
    needs_engine = True
    capabilities = frozenset({"resume", "steer", "screenshots", "file_output"})

    def __init__(
        self,
        policy: ContextPolicy,
        mcp_entry: str,
        internal_url: str,
        token_file: Path,
        *,
        blob_dir: Path | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.policy = policy
        self.mcp_entry = mcp_entry
        self.internal_url = internal_url
        self.token_file = Path(token_file)
        self.blob_dir = blob_dir  # None -> the run's sink.output_dir
        self.clock = clock or SystemClock()
        self._live: dict[RunId, _Live] = {}

    def mcp_config(self, browser: CdpSession, sink: RunSink) -> dict:
        blob_dir = self.blob_dir if self.blob_dir is not None else sink.output_dir
        args = [
            "--cdp-url", browser["cdp_url"],
            "--target-id", browser["target_id"],
            "--run-id", sink.run_id,
            "--internal", self.internal_url,
            "--token-file", str(self.token_file),
            "--blob-dir", str(blob_dir),
            "--policy", getattr(self.policy, "observe", "compressed"),
        ]  # fmt: skip
        return {"mcpServers": {MCP_SERVER: {"command": self.mcp_entry, "args": args}}}

    async def steer(self, handle: HarnessHandle, text: str) -> None:
        """Queue a human instruction; applied at the next tool boundary (at_tool_boundary)."""
        live = self._live.get(handle)
        if live is None:
            raise UnknownHandle(handle)
        live.steers.append(text)

    async def run(
        self,
        task: HarnessTask,
        *,
        browser: CdpSession | None,
        engine: Engine | None,
        sink: RunSink,
    ) -> AsyncIterator[HarnessEvent]:
        """Exactly one terminal, always preceded by a SessionCheckpoint on the sink. Any
        exception before the terminal becomes Error("harness failed: ..."); cancellation
        propagates."""
        live = _Live()
        registered = False
        terminal_sent = False
        try:
            live.carry = _seed_carry(task)
            refusal = self._refusal(browser, engine, sink)
            if refusal is not None:
                inner: AsyncIterator[HarnessEvent] = _just(Error(message=refusal))
            else:
                assert browser is not None and engine is not None
                self._live[sink.run_id] = live
                registered = True
                inner = self._run(task, browser, engine, sink, live)
            async for ev in inner:
                if isinstance(ev, (Done, Error)):
                    if terminal_sent:
                        continue
                    self._checkpoint(sink, live)
                    terminal_sent = True
                yield ev
        except asyncio.CancelledError:
            # interrupted (cancel / takeover): leave the resume state behind, no terminal
            if not terminal_sent:
                with contextlib.suppress(Exception):
                    self._checkpoint(sink, live)
            raise
        except Exception as e:  # noqa: BLE001 - the contract is one terminal, never a raise
            if not terminal_sent:
                with contextlib.suppress(Exception):
                    self._checkpoint(sink, live)
                terminal_sent = True
                yield Error(message=f"harness failed: {type(e).__name__}: {e}")
        finally:
            if registered:
                self._live.pop(sink.run_id, None)

    def _refusal(
        self, browser: CdpSession | None, engine: Engine | None, sink: RunSink
    ) -> str | None:
        if browser is None or engine is None:
            return "own harness needs a browser session and an engine"
        if browser["guard"] == "off":
            return "browser session guard is off: arm it before handing it over"
        if sink.run_id in self._live:
            return f"run {sink.run_id} is already running in this harness"
        return None

    @staticmethod
    def _checkpoint(sink: RunSink, live: _Live) -> None:
        c = live.carry
        sink.record(
            SessionCheckpoint(
                session_id=c.engine_session_id,
                notes=list(c.notes),
                open_tabs=list(c.open_tabs),
                pending_steers=list(live.steers),
            )
        )

    async def _run(
        self,
        task: HarnessTask,
        browser: CdpSession,
        engine: Engine,
        sink: RunSink,
        live: _Live,
    ) -> AsyncIterator[HarnessEvent]:
        """Segment loop. `live.carry` is kept current so the checkpoint sees the latest state."""
        started = self.clock.now()
        mcp_config = self.mcp_config(browser, sink)
        prompt, resume_id = self._first_prompt(task, live)
        while True:
            turn = self._turn(task, started, prompt, mcp_config, resume_id)
            if turn is None:
                yield Error(message="budget_exceeded")
                return
            try:
                run = await engine.start(turn)
            except Exception as e:  # noqa: BLE001 - the stream must end in one Error
                yield Error(message=f"engine start failed: {type(e).__name__}: {e}")
                return
            seg = _Segment()
            async with contextlib.aclosing(self._segment(run, seg, live)) as events:
                async for out in events:
                    yield out
            carry = replace(
                live.carry, engine_session_id=run.session_id or live.carry.engine_session_id
            )
            live.carry = carry
            if seg.cut == "rollover":
                prompt, live.carry = self._rollover(task, carry, sink)
                resume_id = None
                continue
            if isinstance(seg.terminal, Error):
                yield seg.terminal  # quota:*, budget_exceeded, ... unchanged; never a new session
                return
            if seg.terminal is None and seg.cut is None:  # stream ended silently: synthesize Done
                seg.terminal = Done(summary=seg.last_text)
            if seg.ask is not None:
                # unapplied steers stay in live.steers -> SessionCheckpoint.pending_steers
                yield Done(summary=f"waiting_human: {seg.ask.question}")
                return
            if live.steers:  # cut == "steer", or a steer arrived after the last tool boundary
                prompt, resume_id = self._steer_prompt(task, carry, live)
                continue
            assert isinstance(seg.terminal, Done)
            yield seg.terminal
            return

    def _first_prompt(self, task: HarnessTask, live: _Live) -> tuple[str, str | None]:
        """The first segment's prompt and the engine session it resumes (None: a fresh one)."""
        resume = task.get("resume")
        resume_id: str | None = None
        if resume is None:
            prompt = self.policy.compose(task, carry=live.carry)
        elif resume["session_id"]:
            prompt = render_resume_prompt(resume["message"])
            resume_id = resume["session_id"]
        else:  # the old session is gone: fresh session primed with notes/tabs, then the message
            prompt = (
                self.policy.compose(task, carry=live.carry)
                + "\n\n"
                + render_resume_prompt(resume["message"])
            )
        if resume_id is None:
            live.carry = replace(live.carry, est_context_frac=est_tokens(prompt) / CONTEXT_TOKENS)
        return prompt, resume_id

    def _turn(
        self,
        task: HarnessTask,
        started: datetime,
        prompt: str,
        mcp_config: dict,
        resume_id: str | None,
    ) -> EngineTurn | None:
        """The next segment's engine turn, or None once the order's time budget is spent."""
        elapsed = (self.clock.now() - started).total_seconds() / 60
        remaining = task["budget"]["max_minutes"] - elapsed
        if remaining <= 0:
            return None
        return EngineTurn(
            prompt=prompt,
            mcp_config=mcp_config,
            resume_id=resume_id,
            max_minutes=math.ceil(remaining),
            allowed_tools=allowed_tools(),
        )

    async def _segment(
        self, run: EngineRun, seg: _Segment, live: _Live
    ) -> AsyncIterator[HarnessEvent]:
        """Stream one engine run and leave its outcome in `seg.terminal` / `seg.cut`. The run
        is cut right after a ToolResult when a steer is pending or the policy rolls over."""
        stream = run.events()
        try:
            async for ev in stream:
                if seg.terminal is not None:
                    continue  # drain: at most one terminal, let the process exit cleanly
                if isinstance(ev, (Done, Error)):
                    seg.terminal = ev
                    continue
                for out in self._observe(ev, seg):
                    yield out
                if not isinstance(ev, ToolResult):
                    continue
                # --- tool boundary ---
                live.carry = self._after_turn(live.carry, ev, seg, run)
                if seg.ask is not None:
                    continue  # the worker is ending its turn for the human; don't cut
                if live.steers:
                    seg.cut = "steer"
                    break
                if live.carry.engine_session_id is not None and not self.policy.should_resume(
                    live.carry
                ):
                    seg.cut = "rollover"
                    break
        except Exception as e:  # noqa: BLE001
            seg.terminal = seg.terminal or Error(message=f"engine failed: {type(e).__name__}: {e}")
        finally:
            if seg.cut is not None or seg.terminal is None:
                await run.interrupt()
            aclose = getattr(stream, "aclose", None)  # AsyncIterator need not have it
            if aclose is not None:
                await aclose()

    def _rollover(self, task: HarnessTask, carry: Carry, sink: RunSink) -> tuple[str, Carry]:
        """Record `ctx.rollover` and return the fresh session's prompt and carry."""
        policy = self.policy
        reason_of = getattr(policy, "rollover_reason", None)
        reason = (reason_of(carry) if reason_of else None) or "policy"
        prompt = policy.compose(task, carry=carry)
        sink.record(
            ContextRollover(
                policy=policy.id,
                prev_session_id=carry.engine_session_id,
                reason=reason,
                injected_chars=len(prompt),
                injected_tokens=est_tokens(prompt),
            )
        )
        return prompt, replace(
            carry,
            engine_session_id=None,
            turns_in_session=0,
            est_context_frac=est_tokens(prompt) / CONTEXT_TOKENS,
        )

    def _steer_prompt(self, task: HarnessTask, carry: Carry, live: _Live) -> tuple[str, str | None]:
        """Apply every pending steer: the prompt and the session it resumes (None: fresh)."""
        text = "\n".join(live.steers)
        live.steers.clear()
        prompt = render_steer_prompt(text)
        resume_id = carry.engine_session_id
        if resume_id is None:  # engine never reported a session: fresh one, primed
            prompt = self.policy.compose(task, carry=carry) + "\n\n" + prompt
        return prompt, resume_id

    def _observe(self, ev: HarnessEvent, seg: _Segment) -> list[HarnessEvent]:
        """Pass the engine event through, plus derived events (AskRaised, FileOutput)."""
        out: list[HarnessEvent] = [ev]
        if isinstance(ev, Thinking):
            seg.last_text = ev.text
        elif isinstance(ev, TurnUsage):
            seg.usage = ev
        elif isinstance(ev, ToolCall):
            seg.calls[ev.id] = ev
            seg.calls_since_boundary += 1
        elif isinstance(ev, ToolResult):
            call = seg.calls.get(ev.id)
            sent = ev.preview.startswith(ASK_SENT_PREFIX)
            if call is not None and call.name == ASK_TOOL and sent:
                m = _ASK_NUMBER.search(ev.preview)
                seg.ask = AskRaised(
                    question=str(call.args.get("question", "")),
                    irreversible=bool(call.args.get("irreversible", False)),
                    number=int(m.group(1)) if m else None,
                )
                out.append(seg.ask)
            elif (
                call is not None
                and call.name == SCREENSHOT_TOOL
                and ev.ok
                and ev.preview.startswith(SCREENSHOT_SAVED)
            ):
                path = ev.preview[len(SCREENSHOT_SAVED) :].strip()
                out.append(FileOutput(path=path, kind="screenshot"))
        return out

    def _after_turn(self, carry: Carry, ev: ToolResult, seg: _Segment, run: EngineRun) -> Carry:
        m = _URL_LINE.search(ev.preview)
        if m:
            carry = replace(carry, open_tabs=[m.group(1)])  # one tab: the MCP never opens tabs
        carry = replace(carry, engine_session_id=run.session_id or carry.engine_session_id)
        carry = self.policy.after_turn(
            carry,
            TurnSummary(turns=seg.calls_since_boundary, usage=seg.usage, last_text=seg.last_text),
        )
        seg.calls_since_boundary, seg.usage, seg.last_text = 0, None, ""
        return carry
