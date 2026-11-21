import os

from swarm.engine.base import (
    Done,
    Error,
    QuotaSignal,
    ToolCall,
    ToolResult,
    TurnUsage,
)
from swarm.engine.claude import ParseState, build_env, parse_line

from .conftest import FIXTURES


def parse_fixture(name: str):
    st = ParseState()
    evs = []
    for line in (FIXTURES / name).read_text().splitlines():
        evs.extend(parse_line(line, st))
    return evs


def test_parse_init_captures_session_id():
    st = ParseState()
    line = (FIXTURES / "say_hi.jsonl").read_text().splitlines()[0]
    list(parse_line(line, st))
    assert st.session_id


def test_parse_tool_use_and_result_paired():
    evs = parse_fixture("tool_roundtrip.jsonl")
    calls = [e for e in evs if isinstance(e, ToolCall)]
    results = [e for e in evs if isinstance(e, ToolResult)]
    assert calls and results and calls[0].id == results[0].id
    assert calls[0].name == "Bash" and results[0].ok and results[0].preview == "1"


def test_result_success_emits_usage_then_done():
    evs = parse_fixture("say_hi.jsonl")
    assert isinstance(evs[-2], TurnUsage) and isinstance(evs[-1], Done)
    assert evs[-1].summary == "Hi" and evs[-2].output_tokens == 5 and evs[-2].cost_usd > 0


def test_allowed_rate_limit_event_is_not_a_signal():
    assert not [e for e in parse_fixture("say_hi.jsonl") if isinstance(e, QuotaSignal)]


def test_usage_limit_text_becomes_quota_signal_not_error():
    evs = parse_fixture("synthetic_limit.jsonl")
    sigs = [e for e in evs if isinstance(e, QuotaSignal)]
    assert sigs and sigs[0].signal["kind"] == "limit_hit"
    assert sigs[0].signal["reset_at"].timestamp() == 1794538800
    assert not any(isinstance(e, Error) for e in evs)


def test_rejected_rate_limit_event_becomes_limit_signal():
    evs = parse_fixture("synthetic_limit_event.jsonl")
    sig = [e for e in evs if isinstance(e, QuotaSignal)][0].signal
    assert sig["kind"] == "limit_hit" and sig["reset_at"].timestamp() == 1794538800


def test_auth_failure_becomes_auth_failed_signal():
    evs = parse_fixture("synthetic_auth.jsonl")
    sigs = [e for e in evs if isinstance(e, QuotaSignal)]
    assert sigs and sigs[0].signal["kind"] == "auth_failed"
    assert not any(isinstance(e, Error) for e in evs)


def test_bad_line_is_skipped():
    assert list(parse_line("not json{", ParseState())) == []


def test_spawn_env_strips_api_keys(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-should-not-pass")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "tok")
    env = build_env(dict(os.environ))
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_AUTH_TOKEN" not in env
    assert "PATH" in env
