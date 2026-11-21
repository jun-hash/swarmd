"""Hermes front files: the skill carries the forwarding rule verbatim and names only
real SwarmFront tools; the config snippet points at the MCP endpoint swarmd serves."""

import re
from pathlib import Path

from swarm.config import Config
from swarm.front.mcp_server import TOOL_NAMES

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "hermes" / "skills" / "swarm-front" / "SKILL.md"
SNIPPET = ROOT / "hermes" / "config-snippet.yaml"

RULE = (
    "Telegram에서 받은 사람 메시지는 **원문 그대로** `swarm_tell(channel, text)`로 넘긴다. "
    "요약·번역·해석하지 않는다. channel은 `telegram:<chat_id>:<thread_id>`(토픽 없으면 "
    "`telegram:<chat_id>`). 반환된 `ack`를 같은 토픽에 그대로 보낸다. 상태 질문이라도 직접 "
    "답하지 말고 swarm_tell로 넘긴다."
)


def test_skill_has_frontmatter_and_the_rule_verbatim():
    text = SKILL.read_text()
    front = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert front and "name: swarm-front" in front.group(1)
    assert "description:" in front.group(1)
    assert RULE in text


def test_skill_names_only_real_tools_and_all_of_them():
    text = SKILL.read_text()
    assert set(re.findall(r"\bswarm_[a-z]+\b", text)) == set(TOOL_NAMES)


def test_snippet_points_at_the_daemon_mcp_and_excludes_the_swarm_board():
    text = SNIPPET.read_text()
    port = Config.__dataclass_fields__["internal_port"].default
    assert f'url: "http://127.0.0.1:{port}/mcp"' in text
    assert 'Authorization: "Bearer ${SWARM_SECRET}"' in text
    assert "dispatch_profiles:" in text and "- compet" in text
    assert "# verify against your Hermes" in text
