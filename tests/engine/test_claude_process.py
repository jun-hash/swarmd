import asyncio
import json
import os
import signal
import sys
import time

import pytest

from swarm.engine.base import Done, Error, QuotaSignal, ToolCall
from swarm.engine.claude import ClaudeEngine, auth_status, parse_line
from swarm.engine.fake import FakeEngine


def group_gone(pgid: int) -> bool:
    # macOS answers EPERM for a group holding only zombies; both mean "no live members"
    try:
        os.killpg(pgid, 0)
    except (ProcessLookupError, PermissionError):
        return True
    return False


async def wait_pid_gone(pid: int, tries: int = 60) -> bool:
    for _ in range(tries):  # killed orphans linger as zombies until init reaps them
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        await asyncio.sleep(0.05)
    return False


def turn(**kw):
    t = {"prompt": "hi", "mcp_config": {"mcpServers": {}}, "resume_id": None, "max_minutes": 1}
    t.update(kw)
    return t


def engine(binary, tmp_path, **kw):
    return ClaudeEngine(binary=binary, model=None, workdir=tmp_path, **kw)


async def collect(run):
    return [e async for e in run.events()]


async def test_exit_zero_without_done_synthesizes_done(fake_cli, tmp_path):
    run = await engine(fake_cli(), tmp_path).start(turn())
    evs = await collect(run)
    assert isinstance(evs[-1], Done) and len([e for e in evs if isinstance(e, (Done, Error))]) == 1


async def test_nonzero_after_done_keeps_done(fake_cli, tmp_path):
    run = await engine(fake_cli("say_hi.jsonl", exit_code=1), tmp_path).start(turn())
    evs = await collect(run)
    assert isinstance(evs[-1], Done) and not any(isinstance(e, Error) for e in evs)
    assert run.session_id


async def test_nonzero_without_done_is_error_with_stderr_tail(fake_cli, tmp_path):
    run = await engine(fake_cli(exit_code=2, stderr="boom " * 400), tmp_path).start(turn())
    evs = await collect(run)
    assert isinstance(evs[-1], Error) and "boom" in evs[-1].message
    assert len(evs[-1].message) <= 800 + 40


async def test_limit_run_emits_signal_then_terminal_error_marker(fake_cli, tmp_path):
    run = await engine(fake_cli("synthetic_limit.jsonl", exit_code=1), tmp_path).start(turn())
    evs = await collect(run)
    assert any(isinstance(e, QuotaSignal) for e in evs)
    assert isinstance(evs[-1], Error) and evs[-1].message == "quota:limit_hit"


async def test_tool_events_stream_through(fake_cli, tmp_path):
    run = await engine(fake_cli("tool_roundtrip.jsonl"), tmp_path).start(turn())
    evs = await collect(run)
    assert any(isinstance(e, ToolCall) for e in evs) and isinstance(evs[-1], Done)


async def test_interrupt_kills_process_group(fake_cli, tmp_path, monkeypatch):
    calls = []
    real = os.killpg
    monkeypatch.setattr(os, "killpg", lambda pg, sig: (calls.append((pg, sig)), real(pg, sig))[1])
    cli = fake_cli(ignore_term=True, hang=True)
    run = await engine(cli, tmp_path).start(turn())
    for _ in range(100):  # wait until the script has installed its TERM trap
        if os.path.exists(cli + ".ready"):
            break
        await asyncio.sleep(0.05)
    t0 = time.monotonic()
    await run.interrupt()
    dt = time.monotonic() - t0
    assert 1.4 <= dt < 4
    assert [s for _, s in calls if s != 0] == [signal.SIGTERM, signal.SIGKILL]
    assert all(pg > 0 and pg == run.pid for pg, _ in calls)  # killpg(pgid), not killpg(-pid)
    assert group_gone(run.pid)
    evs = await collect(run)
    assert isinstance(evs[-1], Error)


async def test_budget_exceeded_interrupts_and_errors(fake_cli, tmp_path):
    eng = engine(fake_cli(hang=True), tmp_path, seconds_per_minute=0.3)
    run = await eng.start(turn(max_minutes=1))
    evs = await collect(run)
    assert isinstance(evs[-1], Error) and evs[-1].message == "budget_exceeded"
    assert group_gone(run.pid)


async def test_command_line_shape(fake_cli, tmp_path):
    eng = engine(fake_cli(), tmp_path)
    cmd = eng.build_command(turn(resume_id="abc", prompt="do it"), mcp_path="/x.json")
    assert cmd[1:4] == ["-p", "--output-format", "stream-json"] and "--verbose" in cmd
    assert "--bare" not in cmd and "--strict-mcp-config" in cmd
    assert cmd[cmd.index("--resume") + 1] == "abc"
    assert cmd[-2:] == ["--", "do it"]


def test_build_command_takes_allowed_tools_from_the_turn(tmp_path):
    eng = ClaudeEngine(binary="claude", model=None, workdir=tmp_path)
    tools = ["mcp__a__x", "mcp__a__y"]
    cmd = eng.build_command(turn(allowed_tools=tools), mcp_path="/x.json")
    assert cmd[cmd.index("--allowedTools") + 1] == "mcp__a__x,mcp__a__y"
    assert "--allowedTools" not in eng.build_command(turn(), mcp_path="/x.json")
    assert "--allowedTools" not in eng.build_command(turn(allowed_tools=[]), mcp_path="/x.json")


def test_build_command_argv_is_pinned(tmp_path):
    """The argument list of `claude -p` is a contract: order and isolation flags included."""
    tools = [f"mcp__swarmbrowser__{n}" for n in ("browser_goto", "ask_human")]
    eng = ClaudeEngine(binary="claude", model="sonnet", workdir=tmp_path)
    cmd = eng.build_command(
        turn(prompt="P", resume_id="r1", allowed_tools=tools), mcp_path="/m.json"
    )
    assert cmd == [
        "claude", "-p", "--output-format", "stream-json", "--verbose",
        "--mcp-config", "/m.json", "--strict-mcp-config",
        "--setting-sources", "project", "--disable-slash-commands",
        "--allowedTools", "mcp__swarmbrowser__browser_goto,mcp__swarmbrowser__ask_human",
        "--resume", "r1", "--model", "sonnet", "--", "P",
    ]  # fmt: skip
    bare = ClaudeEngine(binary="claude", model=None, workdir=tmp_path)
    assert bare.build_command(turn(prompt="P"), mcp_path="/m.json") == [
        "claude", "-p", "--output-format", "stream-json", "--verbose",
        "--mcp-config", "/m.json", "--strict-mcp-config",
        "--setting-sources", "project", "--disable-slash-commands", "--", "P",
    ]  # fmt: skip


async def test_fake_engine_script_and_single_shot():
    fe = FakeEngine([ToolCall(id="1", name="x", args={}), Done(summary="ok")])
    fe.single_shot_returns({"a": 1}, {"b": 2})
    evs = await collect(await fe.start(turn()))
    assert isinstance(evs[-1], Done)
    assert await fe.single_shot(prompt="p", json_schema={}, timeout_s=1) == {"a": 1}
    assert await fe.single_shot(prompt="p", json_schema={}, timeout_s=1) == {"b": 2}


async def test_single_shot_parses_structured_output(tmp_path):
    p = tmp_path / "cli.sh"
    p.write_text(
        '#!/bin/sh\necho \'{"is_error":false,"result":"{}","structured_output":{"n":3}}\'\n'
    )
    p.chmod(0o755)
    out = await engine(str(p), tmp_path).single_shot(prompt="x", json_schema={}, timeout_s=5)
    assert out == {"n": 3}


async def test_auth_status_parses_json(tmp_path):
    p = tmp_path / "cli.sh"
    p.write_text('#!/bin/sh\necho \'{"loggedIn": true, "authMethod": "claude.ai"}\'\n')
    p.chmod(0o755)
    assert await auth_status(str(p)) is True
    p.write_text("#!/bin/sh\necho '{\"loggedIn\": false}'\nexit 1\n")
    assert await auth_status(str(p)) is False
    assert await auth_status("/nonexistent/claude") is False


@pytest.mark.integration
async def test_real_cli_say_hi(tmp_path):
    run = await engine("claude", tmp_path).start(turn(prompt="say hi in one word"))
    evs = await collect(run)
    assert isinstance(evs[-1], Done) and run.session_id


@pytest.mark.integration
async def test_real_cli_single_shot(tmp_path):
    schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
    out = await engine("claude", tmp_path).single_shot(
        prompt="return n=3", json_schema=schema, timeout_s=90
    )
    assert out == {"n": 3}


async def test_command_has_isolation_args_and_is_tunable(fake_cli, tmp_path):
    cmd = engine(fake_cli(), tmp_path).build_command(turn(), mcp_path="/x.json")
    i = cmd.index("--setting-sources")
    assert cmd[i + 1] == "project" and "--disable-slash-commands" in cmd
    custom = ClaudeEngine(binary="x", model=None, workdir=tmp_path, isolation_args=["--foo"])
    c2 = custom.build_command(turn(), mcp_path="/x.json")
    assert "--foo" in c2 and "--setting-sources" not in c2


async def test_no_second_terminal_after_done_then_budget(fake_cli, tmp_path):
    # prints a result line, then hangs past the budget. The budget leaves room for a slow
    # process start on a loaded machine; the terminal grace window still outlasts it.
    eng = engine(fake_cli("say_hi.jsonl", hang=True), tmp_path, seconds_per_minute=4.0)
    run = await eng.start(turn(max_minutes=1))
    t0 = time.monotonic()
    evs = await collect(run)
    terminals = [e for e in evs if isinstance(e, (Done, Error))]
    assert len(terminals) == 1 and isinstance(terminals[0], Done)
    assert time.monotonic() - t0 < 20  # far below the 30 s the fake CLI hangs for
    assert group_gone(run.pid)


async def test_interrupt_kills_term_ignoring_child_after_leader_exits(fake_cli, tmp_path):
    cli = fake_cli(stubborn_child=True, hang=True)
    run = await engine(cli, tmp_path).start(turn())
    for _ in range(100):
        if os.path.exists(cli + ".child"):
            break
        await asyncio.sleep(0.05)
    child = int(open(cli + ".child").read())
    os.kill(child, 0)  # alive
    await run.interrupt()
    assert await wait_pid_gone(child)
    assert group_gone(run.pid)


async def test_orphaned_group_killed_when_leader_already_reaped(fake_cli, tmp_path):
    # leader exits at once, TERM-ignoring child keeps the group alive
    cli = fake_cli(stubborn_child=True)
    run = await engine(cli, tmp_path).start(turn())
    for _ in range(100):
        if os.path.exists(cli + ".child"):
            break
        await asyncio.sleep(0.05)
    child = int(open(cli + ".child").read())
    await run._proc.wait()
    await run.interrupt()
    assert group_gone(run.pid)
    assert await wait_pid_gone(child)


async def test_no_signals_after_group_observed_gone(fake_cli, tmp_path, monkeypatch):
    run = await engine(fake_cli("say_hi.jsonl"), tmp_path).start(turn())
    await collect(run)
    await run.interrupt()  # observes the group gone and latches
    calls = []
    monkeypatch.setattr(os, "killpg", lambda pg, sig: calls.append((pg, sig)))
    await run.interrupt()
    await run.interrupt()
    assert calls == []


async def test_start_spawns_with_cwd_workdir(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    p = tmp_path / "pwd.sh"
    p.write_text('#!/bin/sh\npwd -P > "$(dirname "$0")/cwd.txt"\n')
    p.chmod(0o755)
    eng = ClaudeEngine(binary=str(p), model=None, workdir=work)
    await collect(await eng.start(turn()))
    assert (tmp_path / "cwd.txt").read_text().strip() == str(work.resolve())


async def test_interrupt_after_completion_is_noop(fake_cli, tmp_path, monkeypatch):
    run = await engine(fake_cli("say_hi.jsonl"), tmp_path).start(turn())
    await collect(run)
    calls = []
    real = os.killpg
    monkeypatch.setattr(os, "killpg", lambda pg, sig: (calls.append((pg, sig)), real(pg, sig))[1])
    await run.interrupt()
    assert [c for c in calls if c[1] != 0] == []


@pytest.mark.integration
async def test_real_cli_isolated_no_hooks(tmp_path):
    run = await engine("claude", tmp_path).start(turn(prompt="say hi in one word"))
    raw = []
    orig = parse_line

    def spy(line, st):
        raw.append(line)
        return orig(line, st)

    import swarm.engine.claude as mod

    mod.parse_line, saved = spy, mod.parse_line
    try:
        evs = await collect(run)
    finally:
        mod.parse_line = saved
    assert isinstance(evs[-1], Done)
    types = [json.loads(line) for line in raw if line.strip().startswith("{")]
    assert not [d for d in types if d.get("type") == "system" and "hook_" in d.get("subtype", "")]


MCP_SERVER = """
import json, sys
for line in sys.stdin:
    m = json.loads(line)
    i, meth = m.get("id"), m.get("method")
    if meth == "initialize":
        r = {"protocolVersion": m["params"]["protocolVersion"], "capabilities": {"tools": {}},
             "serverInfo": {"name": "t", "version": "1"}}
    elif meth == "tools/list":
        r = {"tools": [{"name": "ping", "description": "returns pong",
                        "inputSchema": {"type": "object", "properties": {}}}]}
    elif meth == "tools/call":
        r = {"content": [{"type": "text", "text": "pong"}]}
    elif i is None:
        continue
    else:
        r = {}
    print(json.dumps({"jsonrpc": "2.0", "id": i, "result": r}), flush=True)
"""


@pytest.mark.integration
async def test_real_cli_isolation_keeps_mcp_working(tmp_path):
    srv = tmp_path / "srv.py"
    srv.write_text(MCP_SERVER)
    cfg = {"mcpServers": {"t": {"command": sys.executable, "args": [str(srv)]}}}
    eng = ClaudeEngine(binary="claude", model=None, workdir=tmp_path)
    run = await eng.start(
        turn(
            prompt="call the ping tool",
            mcp_config=cfg,
            max_minutes=3,
            allowed_tools=["mcp__t__ping"],
        )
    )
    evs = await collect(run)
    assert any(isinstance(e, ToolCall) and e.name == "mcp__t__ping" for e in evs)


async def test_interrupt_returns_only_once_the_killed_group_is_gone(
    fake_cli, tmp_path, monkeypatch
):
    """SIGKILL is asynchronous: interrupt() waits (bounded) until the group is really gone,
    so its postcondition holds even when the members take a moment to be reaped."""
    run = await engine(fake_cli("say_hi.jsonl"), tmp_path).start(turn())
    await collect(run)
    run._group_dead = False  # pretend members linger: TERM ignored, KILL takes 3 polls
    monkeypatch.setattr("swarm.engine.claude.KILL_GRACE_S", 0.05)
    state = {"killed": False, "polls_after_kill": 0}

    def fake_killpg(pg, sig):
        if sig == signal.SIGKILL:
            state["killed"] = True
        elif sig == 0 and state["killed"]:
            state["polls_after_kill"] += 1
            if state["polls_after_kill"] > 3:
                raise ProcessLookupError

    monkeypatch.setattr(os, "killpg", fake_killpg)
    await run.interrupt()
    assert state["killed"] and run._group_dead  # observed gone before returning
