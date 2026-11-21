"""`swarm` CLI: init, project create, status, tell, doctor. Network checks use
mock transports; `tell` talks to a real (in-process, faked-adapter) swarmd over MCP."""

import stat

import httpx
import pytest
from conftest import GENERAL, SECRET, TOKEN
from test_daemon import Stack, start_daemon

from swarm import cli
from swarm.config import load_config
from swarm.daemon import load_and_validate

CH = "telegram:-100123:42"


def run_cli(*argv: str) -> int:
    return cli.main(list(argv))


# --- init ---------------------------------------------------------------------------------


def test_init_creates_home_config_and_secret_0600(tmp_path, capsys):
    home = tmp_path / "swarm"
    rc = run_cli(
        "init", "--home", str(home), "--telegram-token", TOKEN, "--general-channel", GENERAL,
        "--chrome", "/Applications/Chrome", "--profile", f"work={home}/chrome/work",
    )  # fmt: skip
    assert rc == 0
    cfg_path, secret = home / "config.toml", home / "secret"
    for p in (cfg_path, secret):
        assert stat.S_IMODE(p.stat().st_mode) == 0o600, p
    assert len(secret.read_text().strip()) >= 32
    cfg = load_and_validate(cfg_path)  # a daemon could start from it
    assert cfg.home == home and cfg.general_channel == GENERAL and cfg.telegram_bot_token == TOKEN
    assert cfg.profiles == {"work": home / "chrome" / "work"}
    for d in ("logs", "blobs", "projects", "runs", "chrome/work"):
        assert (home / d).is_dir(), d
    assert TOKEN not in capsys.readouterr().out


def test_init_is_idempotent_and_never_overwrites(tmp_path):
    home = tmp_path / "swarm"
    assert run_cli("init", "--home", str(home)) == 0
    before = (home / "secret").read_text(), (home / "config.toml").read_text()
    assert run_cli("init", "--home", str(home), "--telegram-token", "other") == 0
    assert ((home / "secret").read_text(), (home / "config.toml").read_text()) == before


def test_init_template_without_values_points_at_the_missing_bits(tmp_path):
    home = tmp_path / "swarm"
    run_cli("init", "--home", str(home))
    cfg = load_config(home / "config.toml")
    assert cfg.telegram_bot_token == "" and cfg.general_channel == ""
    assert cfg.timezone == "Asia/Seoul" and cfg.internal_port == 8765


# --- project create ---------------------------------------------------------------------------


def test_cli_project_create_renders_files(tmp_home, capsys):
    rc = run_cli(
        "--config", str(tmp_home.config), "project", "create", "--slug", "compet",
        "--name", "경쟁사", "--channel", CH, "--objective", "경쟁사 변화를 먼저 안다",
        "--kr", "kr1|가격 변경 감지|5|weekly scan", "--kr", "kr2|리포트 수|4|reports",
        "--weight", "3", "--profile", "work",
    )  # fmt: skip
    assert rc == 0, capsys.readouterr()
    d = tmp_home.home / "projects" / "compet"
    goal = (d / "GOAL.md").read_text()
    assert "경쟁사 변화를 먼저 안다" in goal and "가격 변경 감지" in goal and "리포트 수" in goal
    assert (d / "POLICIES.md").exists() and (d / "MEMORY.md").exists()
    assert (tmp_home.home / "MEMORY.md").exists()
    from swarm.clock import SystemClock
    from swarm.projects.store import SqliteProjectStore
    from swarm.store.blobs import BlobStore
    from swarm.store.db import Database
    from swarm.store.events import SqliteEventLog

    db = Database(tmp_home.home / "swarm.db")
    store = SqliteProjectStore(
        db, SqliteEventLog(db, BlobStore(tmp_home.home / "blobs")), tmp_home.home, SystemClock()
    )
    [p] = store.list()
    assert (p["slug"], p["channel"], p["weight"], p["default_profile"]) == ("compet", CH, 3, "work")
    assert [k["id"] for k in p["goal"]["key_results"]] == ["kr1", "kr2"]
    assert p["goal"]["key_results"][0]["target"] == 5.0
    assert p["default_variant"]["harness"] == "own" and p["board"] == "compet"


@pytest.mark.parametrize(
    "extra",
    [
        ["--kr", "kr1|metric only"],
        ["--kr", "kr1|m|not-a-number|src"],
        ["--weight", "4"],
        ["--profile", "nope"],
        ["--channel", "slack:#x"],
        ["--slug", "Bad Slug"],
    ],
)
def test_cli_project_create_rejects_bad_input(tmp_home, extra, capsys):
    base = {"--slug": "compet", "--name": "n", "--channel": CH, "--objective": "o"}
    args = ["--config", str(tmp_home.config), "project", "create"]
    for k, v in base.items():
        if k not in extra:
            args += [k, v]
    assert run_cli(*args, *extra) == 2
    assert "error" in capsys.readouterr().err.lower()
    assert not (tmp_home.home / "projects" / "compet").exists()


def test_cli_project_create_duplicate_slug_fails(tmp_home, capsys):
    args = ["--config", str(tmp_home.config), "project", "create", "--slug", "compet",
            "--name", "n", "--channel", CH, "--objective", "o"]  # fmt: skip
    assert run_cli(*args) == 0
    assert run_cli(*args) == 1
    assert "compet" in capsys.readouterr().err


# --- status -------------------------------------------------------------------------------------


def test_status_offline_lists_projects_and_says_daemon_down(tmp_home, capsys):
    run_cli("--config", str(tmp_home.config), "project", "create", "--slug", "compet",
            "--name", "경쟁사", "--channel", CH, "--objective", "o")  # fmt: skip
    capsys.readouterr()
    assert run_cli("--config", str(tmp_home.config), "status") == 0
    out = capsys.readouterr().out
    assert "swarmd: not running" in out and "compet" in out and "경쟁사" in out


async def test_status_and_tell_against_a_running_daemon(tmp_home, capsys):
    p = await start_daemon(tmp_home, Stack())
    try:
        cfg = load_config(tmp_home.config)
        text = await cli.status_text(cfg)
        assert "swarmd: running" in text and "heartbeat" in text
        ack = await cli.tell_async(cfg, SECRET, CH, "C사 가격 봐줘")
        assert ack  # the Coordinator's ack, through SwarmFront MCP with the bearer
        assert any(e["payload"].get("text") == "C사 가격 봐줘"
                   for e in p.daemon.log.query(kinds=["human.tell"]))  # fmt: skip
    finally:
        await p.stop()


def test_tell_without_a_daemon_explains(tmp_home, capsys):
    assert run_cli("--config", str(tmp_home.config), "tell", CH, "hi") == 1
    assert "swarmd" in capsys.readouterr().err


# --- doctor ---------------------------------------------------------------------------------------


def doctor_transport(*, telegram_ok=True, hermes_up=True):
    def handle(req: httpx.Request) -> httpx.Response:
        if req.url.host == "api.telegram.org":
            if req.url.path.endswith("/getMe") and telegram_ok:
                return httpx.Response(200, json={"ok": True, "result": {"username": "swarm_bot"}})
            return httpx.Response(401, json={"ok": False, "description": "Unauthorized"})
        if req.url.port == 9:  # hermes_dashboard_url in tmp_home
            if hermes_up:
                return httpx.Response(200, text="<html>hermes</html>")
            raise httpx.ConnectError("refused")
        raise httpx.ConnectError("refused")  # swarmd internal API: not running

    return httpx.MockTransport(handle)


@pytest.fixture
def chrome_bin(tmp_home):
    p = tmp_home.home / "chrome-bin"
    p.write_text("#!/bin/sh\n")
    p.chmod(0o755)
    tmp_home.write_config(chrome_binary=str(p))
    return p


async def test_doctor_all_ok(tmp_home, chrome_bin):
    async def logged_in():
        return True

    checks = await cli.doctor_checks(
        tmp_home.config, auth_check=logged_in, transport=doctor_transport()
    )
    got = {c.name: c.ok for c in checks}
    for name in ("config", "secret", "claude login", "chrome", "hermes dashboard", "telegram"):
        assert got[name] is True, (name, [c for c in checks if c.name == name])
    assert got["swarmd"] is False  # informational: not running here
    assert any("@swarm_bot" in c.detail for c in checks if c.name == "telegram")


async def test_doctor_reports_each_failure_with_a_hint(tmp_home):
    async def logged_out():
        return False

    checks = await cli.doctor_checks(
        tmp_home.config, auth_check=logged_out,
        transport=doctor_transport(telegram_ok=False, hermes_up=False),
    )  # fmt: skip
    bad = {c.name: c for c in checks if not c.ok}
    assert {"claude login", "chrome", "hermes dashboard", "telegram"} <= set(bad)
    assert "claude" in bad["claude login"].hint and "BotFather" in bad["telegram"].hint
    rendered = cli.render_checks(checks)
    assert TOKEN not in rendered and "fail" in rendered and "hint" in rendered


def test_doctor_main_exit_code(tmp_home, chrome_bin, monkeypatch, capsys):
    async def fake_checks(path, **kw):
        return [cli.Check("config", True, "ok"), cli.Check("telegram", False, "401", "hint")]

    monkeypatch.setattr(cli, "doctor_checks", fake_checks)
    assert run_cli("--config", str(tmp_home.config), "doctor") == 1
    out = capsys.readouterr().out
    assert "ok" in out and "fail" in out


async def test_doctor_flags_a_world_readable_config(tmp_home, chrome_bin):
    async def logged_in():
        return True

    tmp_home.config.chmod(0o644)  # it holds the bot token
    checks = await cli.doctor_checks(
        tmp_home.config, auth_check=logged_in, transport=doctor_transport()
    )
    [perm] = [c for c in checks if c.name == "config perms"]
    assert not perm.ok and "chmod 600" in perm.hint
    tmp_home.config.chmod(0o600)
    checks = await cli.doctor_checks(
        tmp_home.config, auth_check=logged_in, transport=doctor_transport()
    )
    assert [c.ok for c in checks if c.name == "config perms"] == [True]
