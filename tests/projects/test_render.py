import pytest

from swarm.projects.render import (
    ParseError,
    parse_goal,
    parse_policies,
    render_goal,
    render_policies,
)


def test_goal_format_is_fixed():
    krs = [{"id": "kr_1", "metric": "m", "target": 5.0, "current": 1.0, "source": "s"}]
    assert render_goal("목표문", krs) == "# 목표\n목표문\n\n## KR\n- kr_1 | m | 5.0 | 1.0\n"


def test_goal_roundtrip():
    krs = [{"id": "kr_1", "metric": "m", "target": 5.0, "current": 1.0, "source": "s"}]
    obj, rows = parse_goal(render_goal("a\nb", krs))
    assert obj == "a\nb" and rows == [("kr_1", "m", 5.0, 1.0)]


@pytest.mark.parametrize(
    "bad", ["<<<", "# 목표\nx\n", "# 목표\nx\n\n## KR\n- kr_1 | m | abc | 1\n"]
)
def test_goal_rejects_garbage(bad):
    with pytest.raises(ParseError):
        parse_goal(bad)


def test_policies_format_and_parse():
    pols = [
        {"id": "pol_1", "text": "t", "active": True},
        {"id": "pol_2", "text": "u", "active": False},
    ]
    txt = render_policies(pols)
    assert txt == "- [pol_1] t\n"
    assert parse_policies(txt) == {"pol_1": "t"}
    with pytest.raises(ParseError):
        parse_policies("free text")
