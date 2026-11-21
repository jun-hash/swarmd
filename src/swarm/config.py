"""~/swarm/config.toml loader."""

import tomllib
from dataclasses import dataclass
from datetime import time
from pathlib import Path

from swarm.types import ChannelRef


@dataclass(frozen=True)
class Config:
    home: Path
    timezone: str
    alert_start: time
    alert_end: time
    telegram_bot_token: str
    general_channel: ChannelRef
    chrome_binary: Path
    profiles: dict[str, Path]
    hermes_dashboard_url: str = "http://127.0.0.1:9119"
    internal_port: int = 8765
    claude_binary: str = "claude"
    claude_model: str | None = None
    hermes_session_token: str | None = None  # sent as a header to the kanban plugin if set
    guard_allowlist: tuple[str, ...] = ()  # hosts whose non-GET requests pass in guarded mode


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def load_config(path: Path) -> Config:
    raw = tomllib.loads(Path(path).read_text())
    return Config(
        home=Path(raw["home"]).expanduser(),
        timezone=raw["timezone"],
        alert_start=_parse_hhmm(raw.get("alert_start", "08:00")),
        alert_end=_parse_hhmm(raw.get("alert_end", "23:00")),
        telegram_bot_token=raw["telegram_bot_token"],
        general_channel=raw["general_channel"],
        hermes_dashboard_url=raw.get("hermes_dashboard_url", "http://127.0.0.1:9119"),
        internal_port=raw.get("internal_port", 8765),
        chrome_binary=Path(raw["chrome_binary"]).expanduser(),
        profiles={k: Path(v).expanduser() for k, v in raw.get("profiles", {}).items()},
        claude_binary=raw.get("claude_binary", "claude"),
        claude_model=raw.get("claude_model"),
        hermes_session_token=raw.get("hermes_session_token") or None,
        guard_allowlist=tuple(raw.get("guard_allowlist", ())),
    )
