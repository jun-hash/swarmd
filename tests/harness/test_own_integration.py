"""Smoke test of the real own stack: headless Chrome + armed guard + real `claude` CLI
(subscription) + real swarm-browser-mcp, against a local static page. Kept to one cheap task."""

import sys
from pathlib import Path

import pytest

from swarm.engine.claude import ClaudeEngine
from swarm.harness.base import Done, ToolCall
from swarm.harness.own.adapter import OwnHarness
from swarm.harness.own.context import RolloverBrief

from .fakes import FakeSink, task

pytestmark = pytest.mark.integration


async def test_own_harness_real_stack_reports_page_title(
    chrome, chrome_session, site, fake_internal, tmp_path
):
    guard = await chrome.open_guard("test", allowlist=["localhost"])
    try:
        await guard.arm(chrome_session, order_id="wo_1", mode="guarded", run_id="run_1")
        token = tmp_path / "token"
        token.write_text("tok-123\n")
        harness = OwnHarness(
            RolloverBrief(),
            mcp_entry=str(Path(sys.executable).parent / "swarm-browser-mcp"),
            internal_url=fake_internal.url,
            token_file=token,
        )
        engine = ClaudeEngine(binary="claude", model="haiku", workdir=tmp_path)
        url = site.url + "/two"
        t = task(
            instruction=f"{url} 을 열고 페이지 제목(title)을 보고하라. 다른 일은 하지 마라.",
            done_criteria=["페이지 제목"],
            start_url=url,
            memory_brief="",
            policies=[],
            budget={"max_minutes": 4},
        )
        sink = FakeSink(tmp_path / "out")
        evs = [e async for e in harness.run(t, browser=chrome_session, engine=engine, sink=sink)]
    finally:
        await guard.close()
    assert isinstance(evs[-1], Done), evs[-3:]
    assert any(isinstance(e, ToolCall) and e.name == "mcp__swarmbrowser__browser_goto" for e in evs)
    assert "Two" in evs[-1].summary
