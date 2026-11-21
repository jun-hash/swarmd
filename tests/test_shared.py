from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from swarm.clock import FakeClock, SystemClock
from swarm.config import load_config
from swarm.ids import new_id


def test_new_id_is_prefixed_and_monotonic():
    a, b = new_id("run"), new_id("run")
    assert a.startswith("run_") and len(a) == 30 and a < b


def test_new_id_monotonic_in_bulk():
    ids = [new_id("x") for _ in range(2000)]
    assert ids == sorted(ids) and len(set(ids)) == 2000


def test_fake_clock_is_tz_aware_and_advances():
    c = FakeClock(datetime(2026, 11, 6, 9, 0, tzinfo=ZoneInfo("Asia/Seoul")))
    c.advance(timedelta(minutes=5))
    assert c.now().tzinfo is not None and c.now().minute == 5


def test_fake_clock_rejects_naive_start():
    import pytest

    with pytest.raises(ValueError):
        FakeClock(datetime(2026, 11, 6, 9, 0))


def test_system_clock_is_seoul_aware():
    assert SystemClock().now().utcoffset() == timedelta(hours=9)


def test_load_config_defaults(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text(
        'home="/tmp/s"\ntimezone="Asia/Seoul"\ntelegram_bot_token="x"\n'
        'general_channel="telegram:-1"\nchrome_binary="/c"\n[profiles]\nwork="/p"\n'
    )
    cfg = load_config(p)
    assert cfg.alert_start == time(8, 0) and cfg.alert_end == time(23, 0)
    assert cfg.internal_port == 8765
    assert cfg.hermes_dashboard_url == "http://127.0.0.1:9119"
    assert cfg.claude_binary == "claude" and cfg.claude_model is None
    assert cfg.hermes_session_token is None  # Hermes kanban auth header off by default
    assert cfg.profiles == {"work": __import__("pathlib").Path("/p")}


def test_load_config_hermes_session_token(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text(
        'home="/tmp/s"\ntimezone="Asia/Seoul"\ntelegram_bot_token="x"\n'
        'general_channel="telegram:-1"\nchrome_binary="/c"\nhermes_session_token="tok"\n'
    )
    assert load_config(p).hermes_session_token == "tok"
