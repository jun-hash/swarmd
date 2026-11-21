"""Fixed markdown formats for the human-readable project files, and their strict parsers."""

import hashlib
import re
from collections.abc import Sequence

from swarm.types import KR, FailedAttempt, Learning, Policy


class ParseError(ValueError):
    """The file does not follow the fixed format; the edit is not applied."""


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _one_line(text: str) -> str:
    return " ".join(text.split())


def clean_metric(metric: str) -> str:
    return metric.replace("\n", " ").replace(" | ", " / ")


# --- GOAL.md ------------------------------------------------------------------------


def render_goal(objective: str, key_results: Sequence[KR]) -> str:
    lines = [
        f"- {k['id']} | {clean_metric(k['metric'])} | {k['target']} | {k['current']}"
        for k in key_results
    ]
    return "# 목표\n" + objective + "\n\n## KR\n" + "".join(x + "\n" for x in lines)


def parse_goal(text: str) -> tuple[str, list[tuple[str, str, float, float]]]:
    head, sep, tail = text.partition("\n\n## KR\n")
    if not sep or not head.startswith("# 목표\n"):
        raise ParseError("GOAL.md: expected '# 목표' and '## KR' sections")
    objective = head[len("# 목표\n") :].strip()
    if not objective:
        raise ParseError("GOAL.md: empty objective")
    rows = []
    for line in tail.splitlines():
        if not line.strip():
            continue
        if not line.startswith("- "):
            raise ParseError(f"GOAL.md: bad KR line: {line!r}")
        parts = [p.strip() for p in line[2:].split(" | ")]
        if len(parts) != 4 or not parts[0] or not parts[1]:
            raise ParseError(f"GOAL.md: bad KR line: {line!r}")
        try:
            rows.append((parts[0], parts[1], float(parts[2]), float(parts[3])))
        except ValueError as e:
            raise ParseError(f"GOAL.md: bad KR number: {line!r}") from e
    return objective, rows


# --- POLICIES.md --------------------------------------------------------------------

_POLICY_LINE = re.compile(r"^- \[([^\]\s]+)\] (.+)$")


def render_policies(policies: Sequence[Policy]) -> str:
    return "".join(f"- [{p['id']}] {_one_line(p['text'])}\n" for p in policies if p["active"])


def parse_policies(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        m = _POLICY_LINE.match(line.rstrip())
        if not m:
            raise ParseError(f"POLICIES.md: bad line: {line!r}")
        out[m.group(1)] = m.group(2)
    return out


# --- project MEMORY.md --------------------------------------------------------------

_LEARN_HDR = "\n\n## 배운 것 (읽기 전용)\n"
_FAIL_HDR = "\n\n## 실패한 시도 (읽기 전용)\n"


def render_project_memory(
    context: str, learnings: Sequence[Learning], failures: Sequence[FailedAttempt]
) -> str:
    return (
        "# 맥락\n"
        + context
        + _LEARN_HDR
        + "".join(f"- {_one_line(x['text'])}\n" for x in learnings)
        + _FAIL_HDR
        + "".join(f"- {_one_line(x['what'])} | {_one_line(x['why'])}\n" for x in failures)
    )


def parse_project_memory(text: str) -> tuple[str, str]:
    """Returns (editable context, read-only lists text) for diffing."""
    head, sep, rest = text.partition(_LEARN_HDR)
    if not sep or not head.startswith("# 맥락\n") or _FAIL_HDR not in rest:
        raise ParseError("MEMORY.md: expected '# 맥락', '## 배운 것', '## 실패한 시도'")
    return head[len("# 맥락\n") :].strip(), rest


# --- global MEMORY.md ---------------------------------------------------------------

_GLOBAL_HDR = "# 전역 기억\n"
_NEW_LINE = re.compile(r"^- ([^\[].*)$")


def render_global_memory(entries: Sequence[tuple[str, str]]) -> str:
    """entries = (id, text) of active entries."""
    return _GLOBAL_HDR + "\n" + "".join(f"- [{i}] {_one_line(t)}\n" for i, t in entries)


def parse_global_memory(text: str) -> tuple[dict[str, str], list[str]]:
    """Returns ({id: text}, [new texts without id]). Lines are `- [id] text` or `- text`."""
    if not text.startswith(_GLOBAL_HDR):
        raise ParseError("MEMORY.md: expected '# 전역 기억' header")
    known: dict[str, str] = {}
    new: list[str] = []
    for line in text[len(_GLOBAL_HDR) :].splitlines():
        if not line.strip():
            continue
        line = line.rstrip()
        if m := _POLICY_LINE.match(line):
            known[m.group(1)] = m.group(2)
        elif m := _NEW_LINE.match(line):
            new.append(m.group(1).strip())
        else:
            raise ParseError(f"MEMORY.md: bad line: {line!r}")
    return known, new
