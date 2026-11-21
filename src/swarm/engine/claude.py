"""ClaudeEngine: drives the official `claude` CLI (subscription login) in stream-json mode.

Observed CLI facts (claude CLI as of 2026-11, see tests/engine/fixtures):
- `--allowedTools` / `--mcp-config` are variadic, so the prompt goes after a `--` separator.
- stream-json needs `--verbose`; events: system/init (session_id), assistant (content blocks
  text|thinking|tool_use), user (tool_result), rate_limit_event, result (is_error, result, usage,
  total_cost_usd). SessionStart hooks of the user add noisy system/hook_* lines (ignored).
- `--output-format json --json-schema` returns a result object with `structured_output`.
- Never `--bare` (skips keychain -> breaks subscription auth).
"""

import asyncio
import contextlib
import json
import logging
import os
import re
import signal
import tempfile
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, tzinfo
from pathlib import Path

from swarm.clock import Clock, SystemClock, zone_of
from swarm.engine.base import (
    Done,
    EngineError,
    EngineEvent,
    EngineTurn,
    Error,
    QuotaSignal,
    Thinking,
    ToolCall,
    ToolResult,
    TurnUsage,
    quota_error,
)
from swarm.types import UsageSignal

log = logging.getLogger(__name__)

STRIPPED_ENV = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
KILL_GRACE_S = 1.5
KILL_REAP_S = 1.0  # after SIGKILL: wait this long for the process group to be gone
STDERR_TAIL = 800
DEFAULT_ISOLATION_ARGS = ["--setting-sources", "project", "--disable-slash-commands"]
SINGLE_SHOT_EXTRA_ARGS = ["--safe-mode"]
TERMINAL_EXIT_GRACE_S = 2.0
PREVIEW_MAX = 300

# ASSUMED text patterns (no real limit/auth failure captured yet; correct here when observed).
# Limit: "Claude AI usage limit reached|<reset epoch seconds>" and variants.
LIMIT_PATTERNS = (
    re.compile(r"usage limit", re.I),
    re.compile(r"rate limit", re.I),
    re.compile(r"limit reached", re.I),
)
RESET_EPOCH_RE = re.compile(r"\|\s*(\d{9,13})\b")
# Auth: expired/missing login or invalid key.
AUTH_PATTERNS = (
    re.compile(r"invalid api key", re.I),
    re.compile(r"please run /login", re.I),
    re.compile(r"not logged in", re.I),
    re.compile(r"oauth token has expired", re.I),
    re.compile(r"authentication_error", re.I),
)
# rate_limit_event.rate_limit_info.status values meaning "fine" (observed: "allowed").
RATE_EVENT_OK = ("allowed", "allowed_warning")
RAW_OUTPUT_MAX = 200  # unparsable CLI output echoed in an EngineError


def build_env(base: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in base.items() if k not in STRIPPED_ENV}


def _epoch_to_dt(v: int | float, tz: tzinfo) -> datetime:
    if v > 1e11:  # milliseconds
        v = v / 1000
    return datetime.fromtimestamp(v, tz)


def classify_text(text: str, clock: Clock) -> UsageSignal | None:
    """Map an error text from the CLI to a UsageSignal, or None."""
    if any(p.search(text) for p in AUTH_PATTERNS):
        return UsageSignal(kind="auth_failed", reset_at=None, at=clock.now())
    if any(p.search(text) for p in LIMIT_PATTERNS):
        m = RESET_EPOCH_RE.search(text)
        reset = _epoch_to_dt(int(m.group(1)), zone_of(clock)) if m else None
        return UsageSignal(kind="limit_hit", reset_at=reset, at=clock.now())
    return None


@dataclass
class ParseState:
    clock: Clock = None  # type: ignore[assignment]
    session_id: str | None = None
    terminal: bool = False  # a Done/Error was emitted
    signal_kind: str | None = None
    last_text: str = ""

    def __post_init__(self) -> None:
        if self.clock is None:
            self.clock = SystemClock()
        self._tool_started: dict[str, float] = {}
        self.mono: Callable[[], float] = time.monotonic


def _preview(content) -> str:
    if isinstance(content, list):
        content = " ".join(b.get("text", "") for b in content if isinstance(b, dict))
    return str(content)[:PREVIEW_MAX]


def parse_line(line: str, st: ParseState) -> Iterator[EngineEvent]:
    line = line.strip()
    if not line:
        return
    try:
        d = json.loads(line)
        if not isinstance(d, dict):
            raise ValueError("not an object")
    except ValueError:
        log.warning("unparsable engine line: %.120s", line)
        return
    try:
        yield from _parse_obj(d, st)
    except Exception:  # noqa: BLE001 - one bad event must not kill the run
        log.warning("failed to interpret engine line: %.120s", line, exc_info=True)


def _signal(st: ParseState, sig: UsageSignal) -> QuotaSignal:
    st.signal_kind = sig["kind"]
    return QuotaSignal(signal=sig)


def _parse_obj(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    t = d.get("type")
    if (
        d.get("session_id")
        and t in ("system", "result")
        and d.get("subtype") in ("init", None, "success")
    ):
        st.session_id = st.session_id or d["session_id"]
    parse = _PARSERS.get(t) if isinstance(t, str) else None  # a malformed type matches none
    if parse is not None:
        yield from parse(d, st)


def _parse_system(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    if d.get("subtype") == "init":
        st.session_id = d.get("session_id") or st.session_id
    yield from ()


def _parse_assistant(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    for b in d.get("message", {}).get("content", []):
        bt = b.get("type")
        if bt == "thinking" and b.get("thinking"):
            yield Thinking(text=b["thinking"])
        elif bt == "text" and b.get("text"):
            st.last_text = b["text"]
            yield Thinking(text=b["text"])
        elif bt == "tool_use":
            st._tool_started[b["id"]] = st.mono()
            yield ToolCall(id=b["id"], name=b["name"], args=b.get("input") or {})


def _parse_user(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    content = d.get("message", {}).get("content", [])
    if not isinstance(content, list):
        return
    for b in content:
        if isinstance(b, dict) and b.get("type") == "tool_result":
            started = st._tool_started.pop(b.get("tool_use_id"), None)
            ms = int((st.mono() - started) * 1000) if started is not None else 0
            yield ToolResult(
                id=b.get("tool_use_id", ""),
                ok=not b.get("is_error", False),
                preview=_preview(b.get("content")),
                ms=ms,
            )


def _parse_rate_limit_event(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    info = d.get("rate_limit_info", {})
    if info.get("status") not in (None, *RATE_EVENT_OK):
        r = info.get("resetsAt")
        reset = _epoch_to_dt(r, zone_of(st.clock)) if isinstance(r, (int, float)) else None
        yield _signal(st, UsageSignal(kind="limit_hit", reset_at=reset, at=st.clock.now()))


def _parse_result(d: dict, st: ParseState) -> Iterator[EngineEvent]:
    text = str(d.get("result") or "")
    if d.get("is_error"):
        sig = classify_text(text, st.clock)
        if sig is None and d.get("api_error_status") in (401, 403):
            sig = UsageSignal(kind="auth_failed", reset_at=None, at=st.clock.now())
        if sig is not None:
            yield _signal(st, sig)
        else:
            st.terminal = True
            yield Error(message=text[:STDERR_TAIL] or "engine error")
        return
    u = d.get("usage") or {}
    yield TurnUsage(
        input_tokens=int(u.get("input_tokens", 0)),
        output_tokens=int(u.get("output_tokens", 0)),
        cached_tokens=int(u.get("cache_read_input_tokens", 0)),
        cost_usd=float(d.get("total_cost_usd") or 0.0),
    )
    st.terminal = True
    yield Done(summary=text)


# one parser per stream-json line type; other types (hook noise, unknown) yield nothing
_PARSERS: dict[str, Callable[[dict, ParseState], Iterator[EngineEvent]]] = {
    "system": _parse_system,
    "assistant": _parse_assistant,
    "user": _parse_user,
    "rate_limit_event": _parse_rate_limit_event,
    "result": _parse_result,
}


class ClaudeRun:
    def __init__(
        self,
        proc: asyncio.subprocess.Process,
        *,
        budget_s: float,
        mcp_path: str,
        clock: Clock,
    ) -> None:
        self._proc = proc
        self.pid = proc.pid
        self._budget_s = budget_s
        self._mcp_path = mcp_path
        self._st = ParseState(clock=clock)
        self._stderr = bytearray()
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        self._interrupted = False
        self._group_dead = False

    @property
    def session_id(self) -> str | None:
        return self._st.session_id

    async def _drain_stderr(self) -> None:
        assert self._proc.stderr is not None
        while chunk := await self._proc.stderr.read(4096):
            self._stderr += chunk
            del self._stderr[:-4096]

    def _killpg(self, sig: int) -> None:
        if self._group_dead:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.pid, sig)  # new session => pgid == pid

    def _group_alive(self) -> bool:
        # A pgid cannot be reused while any member lives, so success means members are ours.
        if self._group_dead:
            return False  # latched: the pgid may be reused now, never touch it again
        try:
            os.killpg(self.pid, 0)
        except (ProcessLookupError, PermissionError):  # EPERM: zombies only / foreign reuse
            self._group_dead = True
            return False
        return True

    async def interrupt(self) -> None:
        self._interrupted = True
        if not self._group_alive():
            return
        self._killpg(signal.SIGTERM)
        loop = asyncio.get_running_loop()
        end = loop.time() + KILL_GRACE_S
        while self._group_alive() and loop.time() < end:
            await asyncio.sleep(0.05)
        if self._group_alive():  # leader may be gone while TERM-ignoring children linger
            self._killpg(signal.SIGKILL)
            # postcondition "group gone": SIGKILL is async, members linger until reaped
            end = loop.time() + KILL_REAP_S
            while self._group_alive() and loop.time() < end:
                await asyncio.sleep(0.02)
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._proc.wait(), 5)

    async def events(self) -> AsyncIterator[EngineEvent]:
        assert self._proc.stdout is not None
        deadline = time.monotonic() + self._budget_s
        terminal_deadline: float | None = None
        try:
            while True:
                if self._st.terminal and terminal_deadline is None:
                    # fixed total window after the terminal event, not reset per line
                    terminal_deadline = time.monotonic() + TERMINAL_EXIT_GRACE_S
                wait = max((terminal_deadline or deadline) - time.monotonic(), 0)
                try:
                    raw = await asyncio.wait_for(self._proc.stdout.readline(), wait)
                except TimeoutError:
                    await self.interrupt()
                    if not self._st.terminal:  # never a second terminal
                        yield Error(message="budget_exceeded")
                        return
                    break
                if not raw:
                    break
                for ev in parse_line(raw.decode("utf-8", "replace"), self._st):
                    yield ev
            rc = await self._proc.wait()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._stderr_task, 2)
            tail = self._stderr.decode("utf-8", "replace").strip()[-STDERR_TAIL:]
            if self._st.terminal:
                if rc != 0:
                    log.warning("claude exited %s after terminal event: %s", rc, tail[-200:])
            elif self._st.signal_kind:
                yield quota_error(self._st.signal_kind)
            elif self._interrupted:
                yield Error(message="interrupted")
            elif rc == 0:
                yield Done(summary=self._st.last_text)
            else:
                yield Error(message=f"claude exited {rc}: {tail}")
        finally:
            if self._proc.returncode is None or self._group_alive():
                await self.interrupt()
            with contextlib.suppress(OSError):
                os.unlink(self._mcp_path)


async def _run_json(binary: str, args: list[str], timeout_s: float, cwd: Path | None) -> bytes:
    proc = await asyncio.create_subprocess_exec(
        binary,
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        env=build_env(dict(os.environ)),
        cwd=cwd,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout_s)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        await proc.wait()
        raise EngineError("single_shot timeout") from None
    if proc.returncode != 0 and not out.strip():
        raise EngineError(
            f"claude exited {proc.returncode}: {err.decode(errors='replace')[-STDERR_TAIL:]}"
        )
    return out


async def auth_status(binary: str) -> bool:
    """True when `claude auth status` reports loggedIn (used hourly by the daemon)."""
    try:
        out = await _run_json(binary, ["auth", "status"], 20, None)
        return bool(json.loads(out).get("loggedIn"))
    except Exception:  # noqa: BLE001
        return False


class ClaudeEngine:
    id = "claude"

    def __init__(
        self,
        binary: str,
        model: str | None,
        workdir: Path,
        *,
        isolation_args: list[str] | None = None,
        seconds_per_minute: float = 60.0,
        clock: Clock | None = None,
    ) -> None:
        # Spawned sessions must not load the user's personal setup (hooks, skills, settings).
        self.isolation_args = (
            list(DEFAULT_ISOLATION_ARGS) if isolation_args is None else list(isolation_args)
        )
        self.binary = binary
        self.model = model
        self.workdir = Path(workdir)
        self._spm = seconds_per_minute
        self._clock = clock or SystemClock()

    def build_command(self, turn: EngineTurn, *, mcp_path: str) -> list[str]:
        cmd = [
            self.binary,
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--mcp-config",
            mcp_path,
            "--strict-mcp-config",
            *self.isolation_args,
        ]
        if allowed := turn.get("allowed_tools"):
            cmd += ["--allowedTools", ",".join(allowed)]
        if turn["resume_id"]:
            cmd += ["--resume", turn["resume_id"]]
        if self.model:
            cmd += ["--model", self.model]
        return [*cmd, "--", turn["prompt"]]  # variadic flags above would swallow a bare prompt

    async def start(self, turn: EngineTurn) -> ClaudeRun:
        fd, mcp_path = tempfile.mkstemp(prefix="swarm-mcp-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(turn["mcp_config"], f)
        try:
            proc = await asyncio.create_subprocess_exec(
                *self.build_command(turn, mcp_path=mcp_path),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
                env=build_env(dict(os.environ)),
                cwd=self.workdir,
                limit=16 * 1024 * 1024,
            )
        except BaseException:
            os.unlink(mcp_path)
            raise
        return ClaudeRun(
            proc, budget_s=turn["max_minutes"] * self._spm, mcp_path=mcp_path, clock=self._clock
        )

    async def single_shot(self, *, prompt: str, json_schema: dict, timeout_s: int) -> dict:
        fd, mcp_path = tempfile.mkstemp(prefix="swarm-mcp-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump({"mcpServers": {}}, f)
        try:
            args = [
                "-p",
                "--output-format",
                "json",
                "--json-schema",
                json.dumps(json_schema),
                "--mcp-config",
                mcp_path,
                "--strict-mcp-config",
                *self.isolation_args,
                *SINGLE_SHOT_EXTRA_ARGS,
            ]
            if self.model:
                args += ["--model", self.model]
            out = await _run_json(self.binary, [*args, "--", prompt], timeout_s, self.workdir)
        finally:
            with contextlib.suppress(OSError):
                os.unlink(mcp_path)
        try:
            d = json.loads(out)
        except ValueError as e:
            raise EngineError(f"unparsable single_shot output: {out[:RAW_OUTPUT_MAX]!r}") from e
        if d.get("is_error"):
            text = str(d.get("result") or "")
            raise EngineError(text[:STDERR_TAIL], classify_text(text, self._clock))
        if isinstance(d.get("structured_output"), dict):
            return d["structured_output"]
        try:
            parsed = json.loads(d.get("result") or "")
        except ValueError as e:
            raise EngineError("single_shot returned no structured output") from e
        if not isinstance(parsed, dict):
            raise EngineError("single_shot output is not an object")
        return parsed
