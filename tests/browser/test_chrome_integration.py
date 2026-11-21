"""ChromeRuntime against a real headless Chrome (one process per profile, target per session)."""

import asyncio
import os
import signal

import pytest

from .pages import Harness

pytestmark = pytest.mark.integration


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def wait_gone(pid: int) -> bool:
    for _ in range(100):
        if not pid_alive(pid):
            return True
        await asyncio.sleep(0.05)
    return False


async def test_open_session_gives_dedicated_target(chrome):
    a = await chrome.open_session(profile="test", purpose="run_a")
    b = await chrome.open_session(profile="test", purpose="run_b")
    assert a["target_id"] != b["target_id"] and a["cdp_url"] == b["cdp_url"]
    assert a["id"] != b["id"]
    assert a["profile"] == "test" and a["guard"] == "off"  # not guarded until arm() succeeds
    assert a["capabilities"] == frozenset(
        {"screenshot", "a11y_tree", "fetch_intercept", "takeover", "multi_tab"}
    )
    assert a["cdp_url"].startswith("ws://127.0.0.1:") and "/devtools/browser/" in a["cdp_url"]
    await chrome.close_session(a["id"])
    await chrome.close_session("nonexistent")  # no exception
    await chrome.close_session(a["id"])  # twice: no exception
    async with Harness(b) as h:  # the other session's target is untouched
        assert await h.evaluate("1+1") == 2


async def test_profiles_get_separate_processes(chrome):
    a = await chrome.open_session(profile="test", purpose="run_a")
    b = await chrome.open_session(profile="other", purpose="run_b")
    assert a["cdp_url"] != b["cdp_url"]
    pids = chrome.pids()
    assert set(pids) == {"test", "other"} and pids["test"] != pids["other"]
    h = await chrome.health()
    assert h["ok"] and h["profiles"] == {"test": True, "other": True} and h["sessions"] == 2
    assert h["version"] and h["version"].startswith("Chrome/")


async def test_closing_last_session_keeps_browser_warm_and_shutdown_kills(chrome):
    a = await chrome.open_session(profile="test", purpose="run_a")
    pid = chrome.pids()["test"]
    await chrome.close_session(a["id"])
    assert pid_alive(pid)
    b = await chrome.open_session(profile="test", purpose="run_b")
    assert b["cdp_url"] == a["cdp_url"] and chrome.pids()["test"] == pid
    await chrome.shutdown()
    assert await wait_gone(pid)
    assert chrome.pids() == {}


async def test_browser_killed_is_relaunched_on_next_session(chrome, log):
    a = await chrome.open_session(profile="test", purpose="run_a")
    pid = chrome.pids()["test"]
    os.killpg(pid, signal.SIGKILL)
    assert await wait_gone(pid)
    await asyncio.sleep(0.2)
    b = await chrome.open_session(profile="test", purpose="run_b")
    assert chrome.pids()["test"] != pid
    assert b["cdp_url"] != a["cdp_url"]
    assert [e["kind"] for e in log.query(kinds=["browser.reconnected"])] == ["browser.reconnected"]
    async with Harness(b) as h:
        assert await h.evaluate("2*3") == 6


async def test_restart_kills_orphan_browser_from_previous_daemon(profile_dirs, log):
    from swarm.browser.chrome import ChromeRuntime

    from .conftest import CHROME

    old = ChromeRuntime(CHROME, profile_dirs, log, headless=True)
    await old.open_session(profile="test", purpose="before_crash")
    orphan = old.pids()["test"]
    assert (profile_dirs["test"] / "SingletonLock").is_symlink()
    new = ChromeRuntime(CHROME, profile_dirs, log, headless=True)  # swarmd restarted
    try:
        s = await new.open_session(profile="test", purpose="after_restart")
        assert await wait_gone(orphan)
        assert new.pids()["test"] != orphan
        async with Harness(s) as h:
            assert await h.evaluate("1+2") == 3
        ev = [e["payload"] for e in log.query(kinds=["browser.orphan_killed"])]
        assert ev == [{"profile": "test", "pid": orphan}]
    finally:
        await new.shutdown()
        await old.shutdown()
