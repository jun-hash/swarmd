"""ChromeRuntime reconnect backoff (5s, 30s, 2m; 3 failures -> browser.disconnected)."""

import asyncio
import socket
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from swarm.browser.chrome import BrowserUnavailable, ChromeRuntime, _Browser


class DeadClient:
    connected = False

    async def close(self) -> None:
        pass


class LiveClient:
    connected = True

    async def send(self, method, params=None, session_id=None):
        assert method == "Target.createTarget"
        return {"targetId": "T-new"}

    async def close(self) -> None:
        pass


def make_runtime(log, delays):
    async def fake_sleep(s):
        delays.append(s)

    rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": Path("/tmp/p")}, log, sleep=fake_sleep)
    rt._browsers["p"] = _Browser(profile="p", proc=None, ws_url="ws://x", client=DeadClient())
    return rt


async def test_three_failed_reconnects_raise_and_log_disconnected(log):
    delays: list[float] = []
    rt = make_runtime(log, delays)
    attempts = []

    async def failing_start(profile):
        attempts.append(profile)
        raise OSError("chrome will not start")

    rt._start = failing_start
    with pytest.raises(BrowserUnavailable):
        await rt.open_session(profile="p", purpose="run_1")
    assert delays == [5, 30, 120]
    assert len(attempts) == 3
    events = list(log.query(kinds=["browser.disconnected"]))
    assert len(events) == 1 and events[0]["payload"]["profile"] == "p"
    assert events[0]["payload"]["attempts"] == 3
    assert "p" not in rt._browsers


async def test_reconnect_succeeds_on_second_attempt(log):
    delays: list[float] = []
    rt = make_runtime(log, delays)
    calls = []

    async def flaky_start(profile):
        calls.append(profile)
        if len(calls) == 1:
            raise OSError("not yet")
        return _Browser(profile=profile, proc=None, ws_url="ws://y", client=LiveClient())

    rt._start = flaky_start
    s = await rt.open_session(profile="p", purpose="run_1")
    assert s["target_id"] == "T-new" and s["cdp_url"] == "ws://y"
    assert delays == [5, 30]
    seen = log.query(kinds=["browser.reconnected", "browser.disconnected"])
    assert [e["kind"] for e in seen] == ["browser.reconnected"]


async def test_unknown_profile_raises_and_close_session_never_raises(log):
    rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": Path("/tmp/p")}, log)
    with pytest.raises(ValueError):
        await rt.open_session(profile="nope", purpose="x")
    await rt.close_session("nonexistent")
    h = await rt.health()
    assert h["browser"] == "chrome" and h["ok"] is False and h["sessions"] == 0


# --- orphan browser from a previous swarmd (SingletonLock) ---------------------------------


def spawn_fake_chrome(user_dir: Path) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", f"--user-data-dir={user_dir}"],
        start_new_session=True,
    )
    # a real orphan is reaped by launchd; reap ours promptly so it does not linger as a zombie
    threading.Thread(target=proc.wait, daemon=True).start()
    return proc


async def wait_exit(proc: subprocess.Popen) -> bool:
    for _ in range(100):
        if proc.poll() is not None:
            return True
        await asyncio.sleep(0.05)
    return False


async def test_orphan_browser_holding_profile_lock_is_killed(log, tmp_path):
    user_dir = tmp_path / "prof"
    user_dir.mkdir()
    orphan = spawn_fake_chrome(user_dir)
    try:
        (user_dir / "SingletonLock").symlink_to(f"{socket.gethostname()}-{orphan.pid}")
        rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": user_dir}, log)
        await rt._kill_orphan("p")
        assert await wait_exit(orphan)
        ev = [e["payload"] for e in log.query(kinds=["browser.orphan_killed"])]
        assert ev == [{"profile": "p", "pid": orphan.pid}]
    finally:
        orphan.kill()
        orphan.wait()


async def test_lock_pointing_at_unrelated_process_is_left_alone(log, tmp_path):
    user_dir = tmp_path / "prof"
    user_dir.mkdir()
    bystander = spawn_fake_chrome(tmp_path / "some-other-profile")
    try:
        (user_dir / "SingletonLock").symlink_to(f"{socket.gethostname()}-{bystander.pid}")
        rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": user_dir}, log)
        await rt._kill_orphan("p")
        await asyncio.sleep(0.2)
        assert bystander.poll() is None
        assert list(log.query(kinds=["browser.orphan_killed"])) == []
        (user_dir / "SingletonLock").unlink()
        (user_dir / "SingletonLock").symlink_to(f"other-host-{bystander.pid}")
        await rt._kill_orphan("p")
        await asyncio.sleep(0.2)
        assert bystander.poll() is None
    finally:
        bystander.kill()
        bystander.wait()


# --- fail-closed when a guard connection is lost ------------------------------------------


class RefusingClient:
    connected = True

    async def send(self, method, params=None, session_id=None, *, timeout_s=None):
        raise ConnectionError("runtime connection broken too")

    async def close(self) -> None:
        pass


async def test_fail_closed_kills_browser_when_targets_cannot_be_closed(log, tmp_path):
    rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": tmp_path}, log)
    rt._browsers["p"] = _Browser(profile="p", proc=None, ws_url="ws://x", client=RefusingClient())
    assert await rt.fail_closed("p", ["T1"]) is True
    assert "p" not in rt._browsers
    assert await rt.fail_closed("unknown-profile", ["T1"]) is False  # nothing was closed
    ev = [e["payload"] for e in log.query(kinds=["browser.fail_closed"])]
    assert ev and ev[0]["killed"] is True and ev[0]["targets"] == ["T1"]


class _Proc:
    returncode = None
    pid = 4242


async def test_session_alive_follows_the_browser_and_its_session(log):
    """A session of a crashed (or dropped) browser reads dead, so the pool reopens."""
    rt = ChromeRuntime(Path("/nonexistent/chrome"), {"p": Path("/tmp/p")}, log)
    proc = _Proc()
    rt._browsers["p"] = _Browser(profile="p", proc=proc, ws_url="ws://x", client=LiveClient())
    s = await rt.open_session(profile="p", purpose="run_1")
    assert rt.session_alive(s["id"]) is True
    proc.returncode = -9  # Chrome was killed
    assert rt.session_alive(s["id"]) is False
    proc.returncode = None
    rt._browsers["p"].client = DeadClient()  # its CDP socket dropped
    assert rt.session_alive(s["id"]) is False
    assert rt.session_alive("bsess_unknown") is False
