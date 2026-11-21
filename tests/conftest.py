"""Shared fixtures: a database, a fake clock and an event log for unit tests, and a temp
~/swarm home for the process layer (daemon, watchdog, CLI)."""

import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest

from swarm.clock import SEOUL, FakeClock
from swarm.store.blobs import BlobStore
from swarm.store.db import Database
from swarm.store.events import SqliteEventLog

GENERAL = "telegram:-100123:1"
TOKEN = "123456:TEST-bot-token"
SECRET = "test-internal-secret"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 11, 6, 10, 0, tzinfo=SEOUL))


@pytest.fixture
def db(tmp_path) -> Database:
    return Database(tmp_path / "swarm.db")


@pytest.fixture
def log(db, tmp_path, clock) -> SqliteEventLog:
    return SqliteEventLog(db, BlobStore(tmp_path / "blobs", clock))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Home:
    home: Path
    config: Path
    port: int

    def write_config(self, **over) -> Path:
        values = {
            "home": str(self.home),
            "timezone": "Asia/Seoul",
            "telegram_bot_token": TOKEN,
            "general_channel": GENERAL,
            "chrome_binary": "/nonexistent/chrome",
            "internal_port": self.port,
            "hermes_dashboard_url": "http://127.0.0.1:9",
        }
        values.update(over)
        lines = []
        for k, v in values.items():
            lines.append(f"{k} = {v}" if isinstance(v, int) else f'{k} = "{v}"')
        lines += ["", "[profiles]", f'work = "{self.home / "chrome" / "work"}"', ""]
        self.config.write_text("\n".join(lines))
        return self.config


@pytest.fixture
def tmp_home(tmp_path) -> Home:
    home = tmp_path / "swarm"
    home.mkdir()
    (home / "secret").write_text(SECRET + "\n")
    (home / "secret").chmod(0o600)
    h = Home(home=home, config=home / "config.toml", port=free_port())
    h.write_config()
    return h
