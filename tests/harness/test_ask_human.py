import json
from pathlib import Path

import pytest

from swarm.harness.own.browser_mcp import BrowserTools


def tools(tmp_path: Path) -> tuple[BrowserTools, list[dict]]:
    t = BrowserTools(
        cdp_url="ws://x",
        target_id="T1",
        run_id="run_1",
        internal="http://127.0.0.1:9",
        token="tok",
        blob_dir=tmp_path,
        policy="compressed",
    )
    sent: list[dict] = []

    async def post(path, body):
        sent.append(json.loads(body))
        return {"ask_id": "ask_1", "outcome": None}

    t._post = post  # type: ignore[method-assign]
    return t, sent


@pytest.mark.parametrize("kind", ["approval", "permission"])
async def test_approval_kinds_are_forced_irreversible(tmp_path, kind):
    t, sent = tools(tmp_path)
    await t.ask_human(kind, "진행할까?", ["예", "아니오"], "예", 30, False)
    d = sent[0]
    assert d["irreversible"] is True and d["default_after"] is None
    assert d["options"] == ["승인", "거절"] and d["recommended"] is None
    assert set(d["forced"]) == {"irreversible", "default_after", "options", "recommended"}


async def test_decision_keeps_default(tmp_path):
    t, sent = tools(tmp_path)
    await t.ask_human("decision", "A?", ["A", "B"], "A", 30, False)
    assert sent[0]["irreversible"] is False and sent[0]["default_after"] is not None
    assert sent[0]["options"] == ["A", "B"] and "forced" not in sent[0]
