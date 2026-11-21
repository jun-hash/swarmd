"""`swarm` CLI: set up the home, add projects, look at the state, poke the running daemon.

  swarm init [--home ~/swarm] [--telegram-token T] [--general-channel C] [--chrome PATH]
             [--profile name=path ...]
      home dirs, config.toml template and the internal API secret (both 0600). Never
      overwrites an existing config or secret.
  swarm project create --slug S --name N --channel telegram:<chat>:<thread> --objective O
             [--kr "id|metric|target|source" ...] [--weight 2] [--profile work]
      writes the project to swarm.db (WAL: fine while swarmd runs; it sees the project at
      its next tick) and renders <home>/projects/<slug>/{GOAL,POLICIES,MEMORY}.md.
  swarm status      offline summary from swarm.db + whether swarmd answers + heartbeat age
  swarm tell <channel> <text>
      local test: the same SwarmFront MCP call Hermes makes (`swarm_tell`, streamable HTTP
      at 127.0.0.1:<internal_port>/mcp with the bearer). Needs a running swarmd.
  swarm doctor      claude login, Chrome, Hermes dashboard, Telegram token, config + secret,
                    swarmd — one ok/fail line each, with a hint
  swarm watchdog    one watchdog pass (what launchd runs every 2 minutes)

`--config PATH` (or $SWARM_CONFIG) selects the config; default ~/swarm/config.toml.
"""

import argparse
import asyncio
import json
import os
import re
import secrets
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from swarm.clock import SystemClock
from swarm.config import Config, load_config
from swarm.front.notifier import parse_channel
from swarm.types import KR, Project

DEFAULT_HOME = "~/swarm"
DEFAULT_CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,39}")
NET_TIMEOUT_S = 3.0
TELEGRAM_API = "https://api.telegram.org"
EXIT_OK, EXIT_FAIL, EXIT_USAGE = 0, 1, 2
HINT_CLAUDE = "run `{binary}` on this machine and log in (subscription, not an API key)"
HINT_HERMES = "start Hermes (`hermes dashboard`) or fix hermes_dashboard_url"
HINT_BOT = "create a bot with @BotFather and put its token in config.toml"
LIST_MAX = 20  # rows `status` shows per list


class UsageError(Exception):
    pass


def _config_path(arg: Path | None) -> Path:
    if arg is not None:
        return Path(arg).expanduser()
    return Path(os.environ.get("SWARM_CONFIG", f"{DEFAULT_HOME}/config.toml")).expanduser()


def _err(msg: str) -> None:
    print(f"swarm: error: {msg}", file=sys.stderr)


def _write_private(path: Path, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    path.chmod(0o600)


def _toml(v: str) -> str:
    return json.dumps(v, ensure_ascii=False)  # a JSON string is a valid TOML basic string


# --- init -------------------------------------------------------------------------------------


def config_template(
    home: Path, *, token: str, general: str, chrome: str, profiles: dict[str, str]
) -> str:
    prof = "\n".join(f"{k} = {_toml(v)}" for k, v in profiles.items())
    return f"""# swarmd config — edit, then run `swarm doctor`.
home = {_toml(str(home))}
timezone = "Asia/Seoul"
alert_start = "08:00"   # non-urgent Telegram messages only between these hours
alert_end = "23:00"

# Telegram bot (from @BotFather). Never share this file (it is 0600).
telegram_bot_token = {_toml(token)}
# The forum's General topic: "telegram:<chat_id>:1". swarmd refuses to start without it
# (Asks whose project topic fails and every system FYI go here).
general_channel = {_toml(general)}

chrome_binary = {_toml(chrome)}
hermes_dashboard_url = "http://127.0.0.1:9119"
internal_port = 8765    # 127.0.0.1 only: internal API + SwarmFront MCP (/mcp)
claude_binary = "claude"
# claude_model = "sonnet"
# hermes_session_token = ""
# hosts whose non-GET requests pass without approval in guarded mode
guard_allowlist = []

[profiles]   # Chrome user-data dirs, one per login identity
{prof}
"""


def cmd_init(a: argparse.Namespace) -> int:
    home = Path(a.home or DEFAULT_HOME).expanduser()
    cfg_path = Path(a.config).expanduser() if a.config else home / "config.toml"
    profiles: dict[str, str] = {}
    for spec in a.profile or []:
        name, sep, path = spec.partition("=")
        if not sep or not name or not path:
            raise UsageError(f"--profile wants name=path, got {spec!r}")
        profiles[name] = str(Path(path).expanduser())
    profiles = profiles or {"work": str(home / "chrome" / "work")}
    home.mkdir(parents=True, exist_ok=True)
    home.chmod(0o700)
    for d in ("logs", "blobs", "projects", "runs", "engine"):
        (home / d).mkdir(exist_ok=True)
    for p in profiles.values():
        Path(p).mkdir(parents=True, exist_ok=True)
    if cfg_path.exists():
        print(f"kept      {cfg_path} (already there)")
    else:
        text = config_template(
            home, token=a.telegram_token or "", general=a.general_channel or "",
            chrome=a.chrome or DEFAULT_CHROME, profiles=profiles,
        )  # fmt: skip
        _write_private(cfg_path, text)
        print(f"wrote     {cfg_path} (0600)")
    secret = home / "secret"
    if secret.exists():
        print(f"kept      {secret} (already there)")
    else:
        _write_private(secret, secrets.token_urlsafe(32) + "\n")
        print(f"wrote     {secret} (0600, internal API bearer)")
    print("next: fill telegram_bot_token + general_channel, then `swarm doctor`")
    return EXIT_OK


# --- project create --------------------------------------------------------------------------


def parse_kr(spec: str) -> KR:
    parts = [p.strip() for p in spec.split("|")]
    if len(parts) != 4 or not all(parts[:2]):
        raise UsageError(f'--kr wants "id|metric|target|source", got {spec!r}')
    try:
        target = float(parts[2])
    except ValueError:
        raise UsageError(f"--kr target must be a number, got {parts[2]!r}") from None
    return KR(id=parts[0], metric=parts[1], target=target, current=0.0, source=parts[3])


def _open_store(cfg: Config):
    from swarm.projects.store import SqliteProjectStore
    from swarm.store.blobs import BlobStore
    from swarm.store.db import Database
    from swarm.store.events import SqliteEventLog

    tz = ZoneInfo(cfg.timezone)
    clock = SystemClock(tz)
    db = Database(cfg.home / "swarm.db")
    log = SqliteEventLog(db, BlobStore(cfg.home / "blobs", clock), tz)
    return db, log, SqliteProjectStore(db, log, cfg.home, clock)


def cmd_project_create(a: argparse.Namespace) -> int:
    from swarm.daemon import default_variant

    cfg = load_config(_config_path(a.config))
    if not SLUG.fullmatch(a.slug):
        raise UsageError(f"--slug must be lowercase letters, digits and '-', got {a.slug!r}")
    try:
        parse_channel(a.channel)
    except ValueError as e:
        raise UsageError(f"--channel: {e}") from None
    if not 1 <= a.weight <= 3:
        raise UsageError(f"--weight must be 1..3, got {a.weight}")
    if a.profile not in cfg.profiles:
        raise UsageError(
            f"--profile {a.profile!r} is not in config [profiles] {list(cfg.profiles)}"
        )
    krs = [parse_kr(k) for k in a.kr or []]
    if not a.objective.strip() or not a.name.strip():
        raise UsageError("--name and --objective must not be empty")
    project = Project(
        id="", slug=a.slug, name=a.name.strip(),
        goal={"objective": a.objective.strip(), "key_results": krs},
        strategy="", policies=[], autonomy=2,
        memory={"context": "", "learnings": [], "failed_attempts": []},
        backlog=[], artifacts=[], channel=a.channel, board=a.slug,
        default_profile=a.profile,
        default_variant=default_variant(cfg, "chrome"),
        weight=a.weight, watches=[],
    )  # fmt: skip
    db, _, store = _open_store(cfg)
    try:
        if any(p["slug"] == a.slug for p in store.list()):
            _err(f"project {a.slug!r} already exists")
            return EXIT_FAIL
        pid = store.create(project)
    finally:
        db.close()
    d = cfg.home / "projects" / a.slug
    print(f"created   {pid} ({a.slug}) -> {a.channel}")
    for name in ("GOAL.md", "POLICIES.md", "MEMORY.md"):
        print(f"rendered  {d / name}")
    return EXIT_OK


# --- status ----------------------------------------------------------------------------------


async def _daemon_up(cfg: Config, transport: httpx.AsyncBaseTransport | None = None) -> bool:
    url = f"http://127.0.0.1:{cfg.internal_port}/internal/health"
    try:
        async with httpx.AsyncClient(timeout=NET_TIMEOUT_S, transport=transport) as c:
            r = await c.get(url)
        return r.status_code == 200 and r.json().get("ok") is True
    except (httpx.HTTPError, ValueError):
        return False


def _heartbeat_age(cfg: Config) -> str:
    try:
        age = time.time() - (cfg.home / "heartbeat").stat().st_mtime
    except FileNotFoundError:
        return "heartbeat: none yet"
    return f"heartbeat: {int(age)}s ago" + (
        "  (stale: watchdog restarts after 300s)" if age > 300 else ""
    )


async def status_text(cfg: Config) -> str:
    lines = [
        f"swarmd: {'running' if await _daemon_up(cfg) else 'not running'}"
        f"  (127.0.0.1:{cfg.internal_port})",
        _heartbeat_age(cfg),
    ]
    if not (cfg.home / "swarm.db").exists():
        lines.append("no swarm.db yet (swarmd has never run, no project created)")
        return "\n".join(lines)
    db, log, store = _open_store(cfg)
    try:
        projects = store.list()
        lines.append(f"projects: {len(projects)}")
        for p in projects:
            counts: dict[str, int] = {}
            for b in p["backlog"]:
                counts[b["status"]] = counts.get(b["status"], 0) + 1
            backlog = ", ".join(f"{k} {v}" for k, v in sorted(counts.items())) or "empty"
            lines.append(f"  {p['slug']:<16} {p['name']}  {p['channel']}  backlog: {backlog}")
        has_runs = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs'"
        ).fetchone()
        rows = db.execute(
            "SELECT id, status, project_id FROM runs WHERE status IN "
            "('leased','running','verifying','waiting_human','interrupted') ORDER BY started_at"
        ).fetchall() if has_runs else []  # fmt: skip
        lines.append(f"runs (unfinished): {len(rows)}")
        lines += [f"  {r['id']}  {r['status']}  {r['project_id'] or '-'}" for r in rows[:LIST_MAX]]
        has_asks = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='asks'"
        ).fetchone()
        asks = db.execute(
            "SELECT number, channel FROM asks WHERE status='open' ORDER BY created_at"
        ).fetchall() if has_asks else []  # fmt: skip
        lines.append(f"open asks: {len(asks)}")
        lines += [f"  #{r['number']}  {r['channel']}" for r in asks[:LIST_MAX]]
    finally:
        db.close()
    return "\n".join(lines)


def cmd_status(a: argparse.Namespace) -> int:
    print(asyncio.run(status_text(load_config(_config_path(a.config)))))
    return EXIT_OK


# --- tell --------------------------------------------------------------------------------------


async def front_call(cfg: Config, secret: str, tool: str, args: dict) -> dict:
    """One SwarmFront MCP tool call (streamable HTTP + bearer), exactly as Hermes makes it.
    Returns the tool's structured result."""
    import httpx2  # the MCP client's HTTP stack
    from mcp import Client
    from mcp.client.streamable_http import streamable_http_client

    url = f"http://127.0.0.1:{cfg.internal_port}/mcp"
    http = httpx2.AsyncClient(headers={"Authorization": f"Bearer {secret}"}, timeout=15)
    async with http, Client(streamable_http_client(url, http_client=http)) as c:
        res = await c.call_tool(tool, args)
    if res.is_error:
        raise RuntimeError(f"{tool} failed: {res.content}")
    return dict(res.structured_content or {})


async def tell_async(cfg: Config, secret: str, channel: str, text: str) -> str:
    """`swarm_tell` over SwarmFront MCP, exactly as Hermes calls it."""
    r = await front_call(cfg, secret, "swarm_tell", {"channel": channel, "text": text})
    return str(r.get("ack", ""))


def cmd_tell(a: argparse.Namespace) -> int:
    cfg = load_config(_config_path(a.config))
    parse_channel(a.channel)
    secret = (cfg.home / "secret").read_text().strip()
    if not asyncio.run(_daemon_up(cfg)):
        _err(f"swarmd is not answering on 127.0.0.1:{cfg.internal_port} (start it first)")
        return EXIT_FAIL
    print(asyncio.run(tell_async(cfg, secret, a.channel, a.text)))
    return EXIT_OK


# --- doctor ------------------------------------------------------------------------------------


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    hint: str = ""


def render_checks(checks: list[Check]) -> str:
    out = []
    for c in checks:
        line = f"{'ok  ' if c.ok else 'fail'}  {c.name:<17} {c.detail}"
        if not c.ok and c.hint:
            line += f"\n      hint: {c.hint}"
        out.append(line)
    return "\n".join(out)


def _scrub(text: str, token: str) -> str:
    return text.replace(token, "<token>") if token else text


def _check_config(config_path: Path) -> tuple[Check, Config | None]:
    """The config check, and the config to run the other checks with (None if unreadable)."""
    from swarm.daemon import ConfigError, load_and_validate

    try:
        cfg = load_and_validate(config_path)
        return Check("config", True, str(config_path)), cfg
    except ConfigError as e:
        check = Check("config", False, str(e), "edit config.toml (or `swarm init`)")
    try:
        return check, load_config(config_path)
    except Exception:  # noqa: BLE001 - nothing else can be checked
        return check, None


def _check_secret(cfg: Config) -> Check:
    secret = cfg.home / "secret"
    if not secret.exists():
        return Check("secret", False, f"{secret} missing", "run `swarm init`")
    if secret.stat().st_mode & 0o077:
        return Check("secret", False, f"{secret} is readable by others", f"chmod 600 {secret}")
    return Check("secret", True, f"{secret} (0600)")


def _check_config_perms(config_path: Path) -> Check:
    loose = config_path.exists() and config_path.stat().st_mode & 0o077
    return Check("config perms", not loose, f"{config_path} holds the bot token",
                 f"chmod 600 {config_path}")  # fmt: skip


async def _check_claude_login(
    cfg: Config, auth_check: Callable[[], Awaitable[bool]] | None
) -> Check:
    from swarm.housekeeping import default_auth_check

    check = auth_check or (lambda: default_auth_check(cfg.claude_binary))
    try:
        logged_in = await asyncio.wait_for(check(), 30)
    except Exception:  # noqa: BLE001
        logged_in = False
    return Check("claude login", logged_in, "subscription login" if logged_in else "not logged in",
                 HINT_CLAUDE.format(binary=cfg.claude_binary))  # fmt: skip


def _check_chrome(cfg: Config) -> Check:
    chrome = cfg.chrome_binary
    chrome_ok = chrome.is_file() and os.access(chrome, os.X_OK)
    return Check("chrome", chrome_ok, str(chrome),
                 "install Google Chrome or fix chrome_binary in config.toml")  # fmt: skip


async def _check_hermes(cfg: Config, c: httpx.AsyncClient) -> Check:
    try:
        r = await c.get(cfg.hermes_dashboard_url)
        up = r.status_code < 500
        detail = f"{cfg.hermes_dashboard_url} -> HTTP {r.status_code}"
    except httpx.HTTPError as e:
        up, detail = False, f"{cfg.hermes_dashboard_url} -> {type(e).__name__}"
    return Check("hermes dashboard", up, detail, HINT_HERMES)


async def _check_telegram(cfg: Config, c: httpx.AsyncClient) -> Check:
    token = cfg.telegram_bot_token
    if not token:
        return Check("telegram", False, "telegram_bot_token is empty", HINT_BOT)
    try:
        r = await c.get(f"{TELEGRAM_API}/bot{token}/getMe", timeout=5.0)
        data = r.json() if r.content else {}
        ok = r.status_code == 200 and bool(data.get("ok"))
        who = (data.get("result") or {}).get("username", "") if ok else ""
        detail = f"bot @{who}" if ok else f"HTTP {r.status_code}"
    except (httpx.HTTPError, ValueError) as e:
        ok, detail = False, type(e).__name__
    return Check("telegram", ok, _scrub(detail, token),
                 "check telegram_bot_token (@BotFather /token)")  # fmt: skip


async def _check_swarmd(cfg: Config, c: httpx.AsyncClient) -> Check:
    try:
        r = await c.get(f"http://127.0.0.1:{cfg.internal_port}/internal/health")
        alive = r.status_code == 200
    except httpx.HTTPError:
        alive = False
    return Check("swarmd", alive, f"127.0.0.1:{cfg.internal_port}",
                 "launchctl kickstart gui/$(id -u)/com.swarm.swarmd")  # fmt: skip


async def doctor_checks(
    config_path: Path,
    *,
    auth_check: Callable[[], Awaitable[bool]] | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[Check]:
    config_check, cfg = _check_config(config_path)
    checks = [config_check]
    if cfg is None:
        return checks
    checks += [_check_secret(cfg), _check_config_perms(config_path)]
    checks.append(await _check_claude_login(cfg, auth_check))
    checks.append(_check_chrome(cfg))
    async with httpx.AsyncClient(timeout=NET_TIMEOUT_S, transport=transport) as c:
        for check in (_check_hermes, _check_telegram, _check_swarmd):
            checks.append(await check(cfg, c))
    return checks


def cmd_doctor(a: argparse.Namespace) -> int:
    checks = asyncio.run(doctor_checks(_config_path(a.config)))
    print(render_checks(checks))
    return EXIT_OK if all(c.ok for c in checks) else EXIT_FAIL


def cmd_watchdog(a: argparse.Namespace) -> int:
    from swarm import watchdog

    return watchdog.main(["--config", str(_config_path(a.config))])


# --- argparse ----------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="swarm", description="swarmd control")
    p.add_argument("--config", type=Path, default=None, help="default ~/swarm/config.toml")
    sub = p.add_subparsers(dest="cmd", required=True)

    i = sub.add_parser("init", help="create the home, config template and secret")
    i.add_argument("--home", default=None, help=f"default {DEFAULT_HOME}")
    i.add_argument(
        "--telegram-token", default=None,
        help="visible in shell history and ps; or leave it out and edit config.toml",
    )  # fmt: skip
    i.add_argument("--general-channel", default=None, help="telegram:<chat_id>:1")
    i.add_argument("--chrome", default=None, help=f"default {DEFAULT_CHROME}")
    i.add_argument("--profile", action="append", help="name=path (repeatable)")
    i.set_defaults(fn=cmd_init)

    pr = sub.add_parser("project", help="projects")
    psub = pr.add_subparsers(dest="pcmd", required=True)
    c = psub.add_parser("create", help="create a project and render its files")
    c.add_argument("--slug", required=True)
    c.add_argument("--name", required=True)
    c.add_argument("--channel", required=True, help="telegram:<chat_id>:<thread_id>")
    c.add_argument("--objective", required=True)
    c.add_argument("--kr", action="append", help='"id|metric|target|source" (repeatable)')
    c.add_argument("--weight", type=int, default=2)
    c.add_argument("--profile", default="work")
    c.set_defaults(fn=cmd_project_create)

    sub.add_parser("status", help="state summary").set_defaults(fn=cmd_status)
    t = sub.add_parser("tell", help="send a message the way Hermes does (local test)")
    t.add_argument("channel")
    t.add_argument("text")
    t.set_defaults(fn=cmd_tell)
    sub.add_parser("doctor", help="check claude, Chrome, Hermes, Telegram").set_defaults(
        fn=cmd_doctor
    )
    sub.add_parser("watchdog", help="one watchdog pass").set_defaults(fn=cmd_watchdog)
    return p


def main(argv: list[str] | None = None) -> int:
    a = build_parser().parse_args(argv)
    try:
        return a.fn(a)
    except UsageError as e:
        _err(str(e))
        return EXIT_USAGE
    except ValueError as e:  # e.g. a bad channel
        _err(str(e))
        return EXIT_USAGE
    except FileNotFoundError as e:
        _err(f"{e.filename or e}: not found (run `swarm init`?)")
        return EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
