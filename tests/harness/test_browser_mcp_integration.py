from pathlib import Path

import pytest

from .helpers import mcp_client

pytestmark = pytest.mark.integration

TOOLS = {
    "browser_goto", "browser_snapshot", "browser_click", "browser_type", "browser_scroll",
    "browser_screenshot", "browser_extract", "browser_tabs", "browser_recall", "ask_human",
    "record_evidence",
}  # fmt: skip


def ref_of(snapshot: str, needle: str) -> str:
    for line in snapshot.splitlines():
        if needle in line and line.startswith("@e"):
            return line.split()[0]
    raise AssertionError(f"{needle!r} not in snapshot:\n{snapshot}")


async def test_tool_names_are_fixed(chrome_session, fake_internal, tmp_path):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        names = {t.name for t in (await mcp.session.list_tools()).tools}
    assert names == TOOLS


async def test_click_by_ref_navigates(chrome_session, fake_internal, site, tmp_path):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        out = await mcp.call("browser_goto", url=site.url + "/")
        assert "Home" in out
        snap = await mcp.call("browser_snapshot")
        out = await mcp.call("browser_click", ref=ref_of(snap, 'link "Go to two"'))
        assert site.url + "/two" in out and "Two" in out
        tabs = await mcp.call("browser_tabs")
        assert "/two" in tabs and "*" in tabs


async def test_type_submit_and_stale_ref(chrome_session, fake_internal, site, tmp_path):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        await mcp.call("browser_goto", url=site.url + "/search")
        snap = await mcp.call("browser_snapshot")
        ref = ref_of(snap, 'textbox "Query"')
        out = await mcp.call("browser_type", ref=ref, text="hello", submit=True)
        assert "/results" in out and "q=hello" in out
        out = await mcp.call("browser_click", ref="@e999")
        assert "browser_snapshot" in out


async def test_extract_truncates_and_scroll_screenshot(
    chrome_session, fake_internal, site, tmp_path
):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        await mcp.call("browser_goto", url=site.url + "/")
        text = await mcp.call("browser_extract")
        assert "filler text" in text and len(text) <= 8100
        assert "Second page" not in text
        assert "scroll" in (await mcp.call("browser_scroll", direction="down")).lower()
        shot = await mcp.call("browser_screenshot")
        path = Path(shot.split()[-1])
        assert path.exists() and path.read_bytes()[:4] == b"\x89PNG"
        assert path.parent == tmp_path / "blobs"


async def test_recall_returns_spilled_text(chrome_session, fake_internal, site, tmp_path):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        await mcp.call("browser_goto", url=site.url + "/")
        await mcp.call(
            "browser_goto",
            url="data:text/html,<title>L</title><a href='/x'>" + "word " * 100 + "</a>",
        )
        snap = await mcp.call("browser_snapshot")
        key = next(w for w in snap.replace("[", " ").split() if w.startswith("#s"))
        full = await mcp.call("browser_recall", ref_id=key)
        assert full.count("word") == 100
        assert "없" in await mcp.call("browser_recall", ref_id="#s99")


async def test_ask_human_returns_end_turn_text_when_no_outcome(
    chrome_session, fake_internal, tmp_path
):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        out = await mcp.call(
            "ask_human", kind="decision", question="A or B?", options=["A", "B"],
            recommended="A", default_after_minutes=30, irreversible=False,
        )  # fmt: skip
    assert out == "질문을 사람에게 보냈습니다(#7). 지금 턴을 끝내세요. 답은 다음 입력으로 옵니다."
    req = fake_internal.requests[0]
    assert req["path"] == "/internal/runs/run_1/asks" and req["auth"] == "Bearer tok-123"
    assert req["body"]["default_after"] == "PT30M" and req["body"]["irreversible"] is False


async def test_ask_human_returns_choice_when_policy_closed_it(
    chrome_session, fake_internal, tmp_path
):
    fake_internal.ask_reply = {
        "ask_id": "a",
        "outcome": {"ask_id": "a", "run_id": "run_1", "status": "answered", "choice": "B",
                    "raw_text": None, "via": "policy"},
    }  # fmt: skip
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        out = await mcp.call(
            "ask_human", kind="approval", question="ok?", options=["A", "B"],
            recommended=None, default_after_minutes=15, irreversible=True,
        )  # fmt: skip
    assert out == "B"
    assert fake_internal.requests[0]["body"]["default_after"] is None  # irreversible


async def test_record_evidence_saves_screenshot_blob(chrome_session, fake_internal, site, tmp_path):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        await mcp.call("browser_goto", url=site.url + "/two")
        out = await mcp.call("record_evidence", url=site.url + "/two", claim="page two exists")
    assert "ev_1" in out
    req = fake_internal.requests[0]
    assert req["path"] == "/internal/runs/run_1/evidence"
    body = req["body"]
    assert body["text"] == "page two exists" and body["url"] == site.url + "/two"
    shot = Path(body["screenshot"])
    assert shot.parent == tmp_path / "blobs" and shot.read_bytes()[:4] == b"\x89PNG"


async def test_blocked_post_reports_approval_needed(
    chrome, chrome_session, fake_internal, site, tmp_path
):
    guard = await chrome.open_guard("test", allowlist=[])
    try:
        await guard.arm(chrome_session, order_id="wo_1", mode="guarded", run_id="run_1")
        async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
            await mcp.call("browser_goto", url=site.url + "/")
            snap = await mcp.call("browser_snapshot")
            await mcp.call("browser_type", ref=ref_of(snap, 'textbox "Name"'), text="x")
            out = await mcp.call("browser_click", ref=ref_of(snap, 'button "Send"'))
        assert "승인이 필요해 차단됐습니다" in out and "ask_human(kind='approval'" in out
        assert site.posts == []
    finally:
        await guard.close()


async def test_failed_post_without_guard_is_not_reported_as_approval(
    chrome_session, fake_internal, site, tmp_path
):
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        await mcp.call("browser_goto", url=site.url + "/dead")
        snap = await mcp.call("browser_snapshot")
        out = await mcp.call("browser_click", ref=ref_of(snap, 'button "Fire"'))
    assert "승인이 필요해" not in out


async def test_ask_human_internal_api_failure_is_not_reported_as_sent(
    chrome_session, fake_internal, tmp_path
):
    fake_internal.stop()  # internal API unreachable
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        out = await mcp.call(
            "ask_human", kind="decision", question="q", options=["a"], irreversible=False
        )
        ev = await mcp.call("record_evidence", url="http://x", claim="c", screenshot=False)
    assert out.startswith("질문 전송 실패") and "보냈습니다" not in out
    assert ev.startswith("증거 기록 실패")


async def test_ask_human_closed_without_choice_is_not_returned_as_choice(
    chrome_session, fake_internal, tmp_path
):
    fake_internal.ask_reply = {
        "ask_id": "a",
        "outcome": {"ask_id": "a", "run_id": "run_1", "status": "expired", "choice": None,
                    "raw_text": None, "via": "default"},
    }  # fmt: skip
    async with mcp_client(chrome_session, internal=fake_internal, tmp_path=tmp_path) as mcp:
        out = await mcp.call(
            "ask_human", kind="decision", question="q", options=["a"], irreversible=False
        )
    assert out != "expired" and "expired" in out and "선택값이 없습니다" in out
