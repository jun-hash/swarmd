"""HarnessAdapter contract, parametrized over every registered adapter (a new adapter
lands here automatically), plus own-harness specifics (ask, steer, rollover, budget, quota)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from swarm.engine.base import QuotaSignal
from swarm.engine.fake import FakeEngine
from swarm.harness import registry
from swarm.harness.base import (
    AskRaised,
    ContextRollover,
    Done,
    Error,
    FileOutput,
    HarnessAdapter,
    SessionCheckpoint,
    Thinking,
    ToolCall,
    ToolResult,
    UnknownHandle,
)
from swarm.harness.own.adapter import OwnHarness, allowed_tools
from swarm.harness.own.browser_mcp import TOOL_NAMES
from swarm.harness.own.context import NaiveResume, RolloverBrief

from .fakes import (
    ASK_SENT,
    GOTO,
    SCRIPT_ASK,
    SCRIPT_OK,
    FakeSink,
    SeqEngine,
    clock,
    fake_session,
    task,
    tool_steps,
)

QUOTA = QuotaSignal(signal={"kind": "limit_hit", "reset_at": None, "at": datetime.now(UTC)})

CONTRACT_SCRIPTS = {
    "ok": SCRIPT_OK,
    "ask": SCRIPT_ASK,
    "error": [Thinking(text="x"), Error(message="boom")],
    "quota": [QUOTA, Error(message="quota:limit_hit")],
    "budget": [Error(message="budget_exceeded")],
    "no_terminal": [Thinking(text="ended silently")],
    "double_terminal": [Done(summary="a"), Done(summary="b")],
}


def deps() -> registry.HarnessDeps:
    return registry.HarnessDeps(
        mcp_entry="swarm-browser-mcp",
        internal_url="http://127.0.0.1:9/",
        token_file=Path("/nonexistent/token"),
        context_policy=RolloverBrief(),
    )


def all_registered_adapters_with_fakes() -> list:
    return [
        pytest.param(factory, id=hid) for hid, factory in sorted(registry.ADAPTER_FACTORIES.items())
    ]


async def collect(adapter: HarnessAdapter, engine, sink, *, browser=None, t=None) -> list:
    browser = browser if browser is not None else fake_session()
    return [
        e
        async for e in adapter.run(
            t or task(),
            browser=browser if adapter.needs_external_browser else None,
            engine=engine if adapter.needs_engine else None,
            sink=sink,
        )
    ]


def terminal_count(evs: list) -> int:
    return sum(isinstance(e, (Done, Error)) for e in evs)


# --- contract (all adapters) --------------------------------------------------------


@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_stream_ends_with_exactly_one_terminal(factory, tmp_path):
    adapter = factory(deps(), None)
    evs = await collect(adapter, FakeEngine(SCRIPT_OK), FakeSink(tmp_path))
    assert terminal_count(evs) == 1 and isinstance(evs[-1], (Done, Error))


@pytest.mark.parametrize("script", list(CONTRACT_SCRIPTS), ids=list(CONTRACT_SCRIPTS))
@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_every_script_ends_with_exactly_one_terminal(factory, script, tmp_path):
    adapter = factory(deps(), None)
    evs = await collect(adapter, FakeEngine(CONTRACT_SCRIPTS[script]), FakeSink(tmp_path))
    assert terminal_count(evs) == 1 and isinstance(evs[-1], (Done, Error))


@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_adapter_declares_contract_fields(factory):
    adapter = factory(deps(), None)
    reg = registry.HarnessRegistry(deps())
    assert reg.get(adapter.id).id == adapter.id
    assert reg.get(adapter.id) is reg.get(adapter.id)  # steer must reach the running instance
    assert adapter.capabilities <= {"resume", "steer", "screenshots", "file_output"}
    assert isinstance(adapter.needs_engine, bool)
    assert isinstance(adapter.needs_external_browser, bool)


@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_engine_start_failure_is_one_error(factory, tmp_path):
    class Broken(FakeEngine):
        async def start(self, turn):
            raise RuntimeError("cannot spawn")

    adapter = factory(deps(), None)
    evs = await collect(adapter, Broken([]), FakeSink(tmp_path))
    assert terminal_count(evs) == 1 and isinstance(evs[-1], Error)


class ComposeBoom(RolloverBrief):
    def compose(self, task, *, carry):
        raise ValueError("compose exploded")


@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_policy_failure_is_one_error(factory, tmp_path):
    d = registry.HarnessDeps(
        mcp_entry="m", internal_url="http://x", token_file=Path("/t"), context_policy=ComposeBoom()
    )
    evs = await collect(factory(d, None), FakeEngine(SCRIPT_OK), FakeSink(tmp_path))
    assert terminal_count(evs) == 1 and isinstance(evs[-1], Error)
    assert evs[-1].message.startswith("harness failed: ValueError: compose exploded")


@pytest.mark.parametrize("script", list(CONTRACT_SCRIPTS), ids=list(CONTRACT_SCRIPTS))
@pytest.mark.parametrize("factory", all_registered_adapters_with_fakes())
async def test_checkpoint_recorded_right_before_terminal(factory, script, tmp_path):
    adapter, sink = factory(deps(), None), FakeSink(tmp_path)
    run = adapter.run(
        task(),
        browser=fake_session() if adapter.needs_external_browser else None,
        engine=FakeEngine(CONTRACT_SCRIPTS[script]) if adapter.needs_engine else None,
        sink=sink,
    )
    async for e in run:
        if isinstance(e, (Done, Error)):
            assert isinstance(sink.records[-1], SessionCheckpoint)
    assert sum(isinstance(r, SessionCheckpoint) for r in sink.records) == 1


def test_registry_unknown_id_and_policy_selection():
    reg = registry.HarnessRegistry(deps())
    with pytest.raises(KeyError):
        reg.get("nope")
    assert isinstance(reg.get("own").policy, RolloverBrief)
    assert isinstance(reg.get("own", context_policy="naive_resume").policy, NaiveResume)
    assert reg.ids() == ["own"]


# --- own harness ----------------------------------------------------------------------


def own(policy=None, **kw) -> OwnHarness:
    return OwnHarness(
        policy or RolloverBrief(),
        mcp_entry="swarm-browser-mcp",
        internal_url="http://127.0.0.1:8765",
        token_file=Path("/tmp/swarm-token"),
        **kw,
    )


def test_own_declares_capabilities():
    h = own()
    assert h.id == "own" and h.needs_external_browser and h.needs_engine
    assert h.capabilities == frozenset({"resume", "steer", "screenshots", "file_output"})


async def test_ask_without_outcome_ends_turn(tmp_path):
    h = own()
    evs = await collect(h, FakeEngine(SCRIPT_ASK), FakeSink(tmp_path, outcome=None))
    asks = [e for e in evs if isinstance(e, AskRaised)]
    assert len(asks) == 1 and asks[0].question == "A or B?" and asks[0].number == 7
    assert isinstance(evs[-1], Done) and evs[-1].summary.startswith("waiting_human")
    assert "A or B?" in evs[-1].summary


async def test_ask_answered_by_policy_is_not_raised(tmp_path):
    script = [
        SCRIPT_ASK[0],
        ToolResult(id="a1", ok=True, preview="A", ms=1),
        Done(summary="went with A"),
    ]
    evs = await collect(own(), FakeEngine(script), FakeSink(tmp_path))
    assert not any(isinstance(e, AskRaised) for e in evs)
    assert evs[-1] == Done(summary="went with A")


async def test_events_pass_through_in_order(tmp_path):
    evs = await collect(own(), FakeEngine(SCRIPT_OK), FakeSink(tmp_path))
    assert evs == SCRIPT_OK


async def test_mcp_config_and_allowed_tools(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    sink = FakeSink(tmp_path / "out")
    await collect(own(), eng, sink)
    cfg = eng.turns[0]["mcp_config"]
    assert cfg == {
        "mcpServers": {
            "swarmbrowser": {
                "command": "swarm-browser-mcp",
                "args": [
                    "--cdp-url", "ws://127.0.0.1:9/devtools/browser/fake",
                    "--target-id", "TARGET1",
                    "--run-id", "run_1",
                    "--internal", "http://127.0.0.1:8765",
                    "--token-file", "/tmp/swarm-token",
                    "--blob-dir", str(tmp_path / "out"),
                    "--policy", "compressed",
                ],
            }
        }
    }  # fmt: skip
    assert allowed_tools() == ["mcp__swarmbrowser__" + n for n in TOOL_NAMES]
    assert eng.turns[0]["resume_id"] is None


async def test_naive_policy_uses_naive_observation(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    await collect(own(NaiveResume()), eng, FakeSink(tmp_path))
    args = eng.turns[0]["mcp_config"]["mcpServers"]["swarmbrowser"]["args"]
    assert args[args.index("--policy") + 1] == "naive"


async def test_first_prompt_is_composed_by_policy(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    t = task()
    await collect(own(), eng, FakeSink(tmp_path), t=t)
    assert eng.turns[0]["prompt"] == RolloverBrief().compose(t, carry=_empty())


def _empty():
    from swarm.harness.own.context import Carry

    return Carry.empty()


async def test_unarmed_browser_session_is_refused(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    evs = await collect(own(), eng, FakeSink(tmp_path), browser=fake_session(guard="off"))
    assert len(evs) == 1 and isinstance(evs[0], Error) and "guard" in evs[0].message
    assert eng.turns == []


async def test_missing_browser_or_engine_is_error(tmp_path):
    h, sink = own(), FakeSink(tmp_path)
    evs = [e async for e in h.run(task(), browser=None, engine=FakeEngine([]), sink=sink)]
    assert len(evs) == 1 and isinstance(evs[0], Error)
    evs = [e async for e in h.run(task(), browser=fake_session(), engine=None, sink=sink)]
    assert len(evs) == 1 and isinstance(evs[0], Error)


async def test_screenshot_result_emits_file_output(tmp_path):
    script = [
        ToolCall(id="s1", name="mcp__swarmbrowser__browser_screenshot", args={}),
        ToolResult(id="s1", ok=True, preview=f"스크린샷 저장: {tmp_path}/shot_1.png", ms=9),
        Done(summary="ok"),
    ]
    evs = await collect(own(), FakeEngine(script), FakeSink(tmp_path))
    files = [e for e in evs if isinstance(e, FileOutput)]
    assert files == [FileOutput(path=f"{tmp_path}/shot_1.png", kind="screenshot")]


# --- steer ----------------------------------------------------------------------------


async def test_steer_applies_at_next_tool_boundary(tmp_path):
    first = [*tool_steps(3), Done(summary="first")]
    eng = SeqEngine([first, [Thinking(text="ok"), Done(summary="second")]])
    h, sink = own(), FakeSink(tmp_path)
    evs = []
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        evs.append(e)
        if isinstance(e, ToolCall) and e.id == "t0":
            await h.steer(sink.run_id, "요금제 페이지만 봐")
    assert len(eng.turns) == 2 and eng.runs[0].interrupted
    assert ToolResult(id="t0", ok=True, preview="url: http://site.test/0", ms=1) in evs
    assert not any(isinstance(e, ToolCall) and e.id == "t1" for e in evs)  # cut at boundary
    assert eng.turns[1]["resume_id"] == "sess-1"  # same engine session
    assert eng.turns[1]["allowed_tools"] == allowed_tools()  # every turn, resumes included
    assert eng.turns[1]["prompt"].startswith("사람 지시: 요금제 페이지만 봐")
    assert evs[-1] == Done(summary="second") and terminal_count(evs) == 1


async def test_steer_pending_when_engine_finishes_resumes_once(tmp_path):
    eng = SeqEngine([[Thinking(text="almost"), Done(summary="first")], [Done(summary="second")]])
    h, sink = own(), FakeSink(tmp_path)
    evs = []
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        evs.append(e)
        if isinstance(e, Thinking):
            await h.steer(sink.run_id, "표도 첨부해")
    assert len(eng.turns) == 2 and "사람 지시: 표도 첨부해" in eng.turns[1]["prompt"]
    assert evs[-1] == Done(summary="second") and terminal_count(evs) == 1


async def test_steer_unknown_handle_raises():
    with pytest.raises(UnknownHandle):
        await own().steer("run_nope", "x")


async def test_steer_after_run_finished_raises(tmp_path):
    h, sink = own(), FakeSink(tmp_path)
    await collect(h, FakeEngine(SCRIPT_OK), sink)
    with pytest.raises(UnknownHandle):
        await h.steer(sink.run_id, "late")


# --- rollover -------------------------------------------------------------------------


async def test_rollover_starts_new_session_with_brief_and_records_event(tmp_path):
    eng = SeqEngine([[*tool_steps(5), Done(summary="first")], [Done(summary="second")]])
    sink = FakeSink(tmp_path)
    evs = await collect(own(RolloverBrief(rollover_turns=2)), eng, sink)
    assert len(eng.turns) == 2 and eng.runs[0].interrupted
    assert eng.turns[1]["resume_id"] is None  # new session
    assert "진행 노트" in eng.turns[1]["prompt"] and "step 0 done" in eng.turns[1]["prompt"]
    assert "http://site.test/1" in eng.turns[1]["prompt"]  # open tab carried over
    rolls = [r for r in sink.records if isinstance(r, ContextRollover)]
    assert len(rolls) == 1
    r = rolls[0]
    assert r.kind == "ctx.rollover" and r.prev_session_id == "sess-1" and r.reason == "turns"
    assert r.injected_chars == len(eng.turns[1]["prompt"]) and r.injected_tokens > 0
    assert evs[-1] == Done(summary="second") and terminal_count(evs) == 1
    assert not any(isinstance(e, ContextRollover) for e in evs)  # sink only, not the stream


async def test_naive_resume_never_rolls_over(tmp_path):
    eng = FakeEngine([*tool_steps(60), Done(summary="done")])
    sink = FakeSink(tmp_path)
    evs = await collect(own(NaiveResume()), eng, sink)
    assert len(eng.turns) == 1 and evs[-1] == Done(summary="done")
    assert not any(isinstance(r, ContextRollover) for r in sink.records)


# --- budget (engine enforces max_minutes; harness passes the remaining minutes) -------


async def test_budget_exceeded_from_engine_is_terminal(tmp_path):
    eng = FakeEngine([*tool_steps(1), Error(message="budget_exceeded")])
    evs = await collect(own(), eng, FakeSink(tmp_path))
    assert evs[-1] == Error(message="budget_exceeded") and len(eng.turns) == 1
    assert eng.turns[0]["max_minutes"] == 60


async def test_remaining_minutes_passed_to_each_engine_turn(tmp_path):
    c = clock()
    eng = SeqEngine(
        [[*tool_steps(2), Done(summary="a")], [Done(summary="b")]],
        on_start=lambda _t: c.advance(timedelta(minutes=30)),
    )
    h, sink = own(clock=c), FakeSink(tmp_path)
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        if isinstance(e, ToolCall) and e.id == "t0":
            await h.steer(sink.run_id, "계속")
    assert [t["max_minutes"] for t in eng.turns] == [60, 30]


async def test_budget_minutes_exceeded_interrupts(tmp_path):
    c = clock()
    eng = SeqEngine(
        [[*tool_steps(2), Done(summary="a")], [Done(summary="b")]],
        on_start=lambda _t: c.advance(timedelta(minutes=60)),
    )
    h, sink = own(clock=c), FakeSink(tmp_path)
    evs = []
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        evs.append(e)
        if isinstance(e, ToolCall) and e.id == "t0":
            await h.steer(sink.run_id, "계속")
    assert len(eng.turns) == 1 and eng.runs[0].interrupted  # no new turn after 60 min
    assert evs[-1] == Error(message="budget_exceeded") and terminal_count(evs) == 1


# --- quota ----------------------------------------------------------------------------


async def test_quota_signal_and_error_pass_through_without_new_session(tmp_path):
    eng = SeqEngine([[QUOTA, Error(message="quota:limit_hit")], [Done(summary="never")]])
    h, sink = own(RolloverBrief(rollover_turns=1)), FakeSink(tmp_path)
    evs = []
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        evs.append(e)
        if isinstance(e, QuotaSignal):
            await h.steer(sink.run_id, "pending steer must not resume after quota")
    assert evs == [QUOTA, Error(message="quota:limit_hit")]
    assert len(eng.turns) == 1


async def test_ask_sent_text_constant_matches_mcp_server():
    from swarm.harness.own.browser_mcp import ASK_SENT_PREFIX

    assert ASK_SENT.startswith(ASK_SENT_PREFIX)


async def test_goto_toolcall_name_is_namespaced():
    assert GOTO == "mcp__swarmbrowser__browser_goto" and "browser_goto" in TOOL_NAMES


async def test_tool_names_match_mcp_server(tmp_path):
    from swarm.harness.own.browser_mcp import BrowserTools, build_server

    tools = BrowserTools(
        cdp_url="ws://x", target_id="t", run_id="r", internal="http://x", token="k",
        blob_dir=tmp_path, policy="compressed",
    )  # fmt: skip
    names = {t.name for t in await build_server(tools).list_tools()}
    assert names == set(TOOL_NAMES) and len(TOOL_NAMES) == len(set(TOOL_NAMES))


# --- resume / checkpoint (own) --------------------------------------------------------


def checkpoints(sink: FakeSink) -> list[SessionCheckpoint]:
    return [r for r in sink.records if isinstance(r, SessionCheckpoint)]


async def test_resume_with_session_id_resumes_with_message(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    resume = {"session_id": "old-sess", "message": "사람 답: B", "notes": ["n1"], "open_tabs": []}
    evs = await collect(own(), eng, FakeSink(tmp_path), t=task(resume=resume))
    assert eng.turns[0]["resume_id"] == "old-sess"
    assert eng.turns[0]["prompt"].startswith("사람 답: B")
    assert "browser_snapshot으로 현재 화면을 다시 확인" in eng.turns[0]["prompt"]
    assert evs[-1] == SCRIPT_OK[-1]


async def test_resume_without_session_id_composes_notes_tabs_and_message(tmp_path):
    eng = FakeEngine(SCRIPT_OK)
    resume = {
        "session_id": None,
        "message": "브라우저가 다시 열림. 마지막 URL: http://site.test/pricing",
        "notes": ["요금제 페이지 찾음"],
        "open_tabs": ["http://site.test/pricing"],
    }
    await collect(own(), eng, FakeSink(tmp_path), t=task(resume=resume))
    p = eng.turns[0]["prompt"]
    assert eng.turns[0]["resume_id"] is None
    assert "요금제 페이지 찾음" in p and "- http://site.test/pricing" in p
    assert p.rstrip().endswith("이어가라.") and "브라우저가 다시 열림" in p
    assert "현재 탭에서 이어서 진행" in p and "browser_goto로 http://site.test/ 을" not in p


async def test_checkpoint_on_waiting_human_keeps_pending_steer(tmp_path):
    h, sink = own(), FakeSink(tmp_path)
    evs = []
    async for e in h.run(task(), browser=fake_session(), engine=FakeEngine(SCRIPT_ASK), sink=sink):
        evs.append(e)
        if isinstance(e, AskRaised):
            await h.steer(sink.run_id, "표도 첨부해")
    assert evs[-1].summary.startswith("waiting_human")
    [cp] = checkpoints(sink)
    assert cp.session_id == "fake-session" and cp.pending_steers == ["표도 첨부해"]


async def test_checkpoint_on_quota_and_done(tmp_path):
    sink = FakeSink(tmp_path)
    await collect(own(), FakeEngine([QUOTA, Error(message="quota:limit_hit")]), sink)
    [cp] = checkpoints(sink)
    assert cp.session_id == "fake-session" and cp.pending_steers == []
    sink = FakeSink(tmp_path)
    await collect(own(), FakeEngine(SCRIPT_OK), sink)
    [cp] = checkpoints(sink)
    assert cp.session_id == "fake-session" and cp.open_tabs == ["http://site.test/"]


async def test_checkpoint_on_refusal_carries_resume_state(tmp_path):
    sink = FakeSink(tmp_path)
    resume = {"session_id": "s9", "message": "m", "notes": ["n"], "open_tabs": ["http://t/"]}
    await collect(own(), FakeEngine([]), sink, browser=fake_session(guard="off"),
                  t=task(resume=resume))  # fmt: skip
    [cp] = checkpoints(sink)
    assert (cp.session_id, cp.notes, cp.open_tabs) == ("s9", ["n"], ["http://t/"])


async def test_rollover_without_reason_method_falls_back(tmp_path):
    class Minimal:
        id = "minimal"
        observe = "compressed"

        def compose(self, task, *, carry):
            return "go"

        def should_resume(self, carry):
            return carry.turns_in_session < 1

        def after_turn(self, carry, turn):
            from dataclasses import replace

            return replace(carry, turns_in_session=carry.turns_in_session + turn.turns)

        def compress_observation(self, obs):
            return obs["text"]

    eng = SeqEngine([[*tool_steps(2), Done(summary="a")], [Done(summary="b")]])
    sink = FakeSink(tmp_path)
    evs = await collect(own(Minimal()), eng, sink)
    [roll] = [r for r in sink.records if isinstance(r, ContextRollover)]
    assert roll.reason == "policy" and evs[-1] == Done(summary="b")


async def test_steer_text_is_capped_and_asks_for_snapshot(tmp_path):
    eng = SeqEngine([[*tool_steps(2), Done(summary="a")], [Done(summary="b")]])
    h, sink = own(), FakeSink(tmp_path)
    async for e in h.run(task(), browser=fake_session(), engine=eng, sink=sink):
        if isinstance(e, ToolCall) and e.id == "t0":
            await h.steer(sink.run_id, "a" * 5000)
            await h.steer(sink.run_id, "마지막 지시")
    p = eng.turns[1]["prompt"]
    assert "마지막 지시" in p and len(p) < 2_300
    assert "browser_snapshot으로 현재 화면을 다시 확인" in p


async def test_cancelled_run_records_checkpoint(tmp_path):
    """Takeover interrupts the stream: the checkpoint must still carry session and steers."""
    import asyncio

    from swarm.engine.fake import FakeRun

    class Hang(FakeRun):
        async def events(self):
            for ev in tool_steps(1):
                yield ev
            await asyncio.Event().wait()

    class HangEngine(FakeEngine):
        async def start(self, turn):
            self.turns.append(turn)
            return Hang([], "sess-hang")

    h, sink = own(), FakeSink(tmp_path)
    seen = asyncio.Event()

    async def consume():
        async for e in h.run(task(), browser=fake_session(), engine=HangEngine([]), sink=sink):
            if isinstance(e, Thinking):
                seen.set()

    t = asyncio.create_task(consume())
    await seen.wait()
    await h.steer(sink.run_id, "표도 첨부해")
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    [cp] = checkpoints(sink)
    assert cp.session_id == "sess-hang" and cp.pending_steers == ["표도 첨부해"]


async def test_closing_the_segment_loop_interrupts_the_engine_run_before_returning(tmp_path):
    """Closing the segment loop mid-segment interrupts the engine run and closes its stream
    before `aclose()` returns, not later when the inner stream happens to be finalized."""
    import asyncio

    from swarm.engine.fake import FakeRun
    from swarm.harness.own.adapter import _Live

    order: list[str] = []

    class Hang(FakeRun):
        async def events(self):
            try:
                for ev in tool_steps(1):
                    yield ev
                await asyncio.Event().wait()
            finally:
                order.append("stream_closed")

        async def interrupt(self):
            order.append("interrupt")
            await super().interrupt()

    class HangEngine(FakeEngine):
        async def start(self, turn):
            return Hang([], "sess-hang")

    h, sink = own(), FakeSink(tmp_path)
    loop = h._run(task(), fake_session(), HangEngine([]), sink, _Live())
    async for ev in loop:
        if isinstance(ev, Thinking):
            break  # suspended mid-segment
    await loop.aclose()
    order.append("closed")
    assert order == ["interrupt", "stream_closed", "closed"]
