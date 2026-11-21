import stat
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fake_cli(tmp_path):
    """Factory: shell script that prints fixture lines then exits with a code."""

    def make(
        fixture: str | None = None,
        *,
        exit_code: int = 0,
        ignore_term: bool = False,
        hang: bool = False,
        stderr: str = "",
        stubborn_child: bool = False,
    ) -> str:
        body = ["#!/bin/sh"]
        if ignore_term:
            body.append("trap '' TERM")
            body.append('touch "$0.ready"')
        if stderr:
            body.append(f"echo '{stderr}' >&2")
        if stubborn_child:  # background child that ignores TERM, outlives the leader
            body.append('sh -c \'echo $$ > "$0.child"; trap "" TERM; exec sleep 30\' "$0" &')
            body.append("sleep 30 &")
        if fixture:
            body.append(f"cat '{FIXTURES / fixture}'")
        if hang:
            body.append("sleep 30")
        body.append(f"exit {exit_code}")
        p = tmp_path / f"fake_claude_{len(list(tmp_path.iterdir()))}.sh"
        p.write_text("\n".join(body) + "\n")
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
        return str(p)

    return make
