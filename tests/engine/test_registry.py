from datetime import UTC, datetime

import pytest

from swarm.clock import FakeClock
from swarm.config import load_config
from swarm.engine.claude import ClaudeEngine
from swarm.engine.registry import ENGINE_FACTORIES, build_engine

CLOCK = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))


def make_config(tmp_path, extra=""):
    p = tmp_path / "config.toml"
    p.write_text(
        'home="/tmp/s"\ntimezone="Asia/Seoul"\ntelegram_bot_token="x"\n'
        'general_channel="telegram:-1"\nchrome_binary="/c"\n' + extra + '[profiles]\nwork="/p"\n'
    )
    return load_config(p)


def test_build_engine_claude_from_config(tmp_path):
    cfg = make_config(tmp_path, 'claude_binary="/opt/claude"\nclaude_model="sonnet"\n')
    eng = build_engine("claude", cfg, tmp_path, CLOCK)
    assert isinstance(eng, ClaudeEngine)
    assert eng.binary == cfg.claude_binary and eng.model == "sonnet" and eng.workdir == tmp_path


def test_build_engine_unknown_raises_keyerror(tmp_path):
    with pytest.raises(KeyError):
        build_engine("nope", make_config(tmp_path), tmp_path, CLOCK)


def test_every_registered_factory_builds_an_engine_with_its_own_id(tmp_path):
    for engine_id in ENGINE_FACTORIES:
        assert build_engine(engine_id, make_config(tmp_path), tmp_path, CLOCK).id == engine_id
