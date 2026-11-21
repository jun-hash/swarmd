"""Cards: a backlog item or a run as a kanban card, and a human change as a Coordinator
event. Pure mapping, no I/O.

A card's body is the intent, the done criteria and the latest summary; links and artifact
paths go in as lines because the attachment upload API is unverified. Run status
maps to card status by `RUN_CARD_STATUS`. Of the human's changes, only a reorder, an archive
and a comment become Coordinator events: the mirror puts every other move back itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import get_args

from swarm.text import clip
from swarm.types import (
    BacklogItem,
    CardStatus,
    CardView,
    Event,
    KanbanChange,
    Project,
    ProjectId,
    RunResult,
    WorkerRun,
    WorkOrder,
)

CARD_STATUSES = frozenset(get_args(CardStatus))
BODY_MAX = 4_000
CARD_TITLE_MAX = 120  # a card title on the board
ARCHIVE_AFTER = timedelta(days=14)
LINKS_MAX = 10

# run status -> card status; failed and interrupted follow what the Coordinator does to a
# backlog item after such a run
RUN_CARD_STATUS: dict[str, CardStatus] = {
    "leased": "running",
    "running": "running",
    "verifying": "running",
    "waiting_human": "blocked",
    "done": "done",
    "cancelled": "archived",
    "failed": "blocked",
    "interrupted": "ready",
}
FEED_KINDS = (
    "project.backlog_added",
    "project.backlog_updated",
    "project.backlog_reprioritized",
    "ask.raised",
    "run.finished",
    *(f"run.{s}" for s in RUN_CARD_STATUS),
)
_EVENT_KIND = {
    "reordered": "human.kanban_reorder",
    "archived": "human.kanban_archive",
    "commented": "human.kanban_comment",
}


def _body(intent: str, criteria: Sequence[str], extra: Sequence[str] = ()) -> str:
    """Intent, done criteria and `extra` lines, clipped to BODY_MAX."""
    lines = [intent.strip()]
    if criteria:
        lines += ["", "완료 기준:", *(f"- {c}" for c in criteria)]
    return clip("\n".join([*lines, *extra]), BODY_MAX)


def card_from_backlog(
    item: BacklogItem,
    project: Project,
    *,
    summary: str = "",
    links: Sequence[str] = (),
    attachments: Sequence[Path] = (),
) -> CardView:
    """A backlog item's card (key = item id, which the Coordinator's `find_item` resolves).
    Body = intent + done criteria + latest summary (item note / latest run summary); links
    and artifact paths are added as lines when sent."""
    status = "archived" if item["status"] == "dropped" else item["status"]
    extra = ["", f"최근 요약: {summary}"] if summary else []
    return CardView(
        key=item["id"], board=project["board"], title=clip(item["title"], CARD_TITLE_MAX),
        body=_body(item["intent"], item["done_criteria"], extra), status=status,  # type: ignore[typeddict-item]
        priority=int(item["priority"]), links=list(links), attachments=list(attachments),
    )  # fmt: skip


def card_from_run(
    run: WorkerRun,
    order: WorkOrder,
    project: Project,
    *,
    summary: str = "",
    links: Sequence[str] = (),
    attachments: Sequence[Path] = (),
) -> CardView:
    """A card for a run without a backlog item (discover, improve ...); key = run id. A run
    of a backlog item shows on that item's card, whose status the Coordinator keeps."""
    lines = order["intent"].strip().splitlines()
    first = lines[0] if lines else order["id"]
    extra = ["", f"최근 요약: {summary}"] if summary else []
    return CardView(
        key=run["id"], board=project["board"],
        title=clip(f"[{order['kind']}] {first}", CARD_TITLE_MAX),
        body=_body(order["intent"], order["done_criteria"], extra),
        status=RUN_CARD_STATUS.get(run["status"], "running"), priority=int(order["priority"]),
        links=list(links), attachments=list(attachments),
    )  # fmt: skip


def change_event(
    change: KanbanChange, *, project_id: ProjectId | None, at: datetime
) -> Event | None:
    """The Coordinator event for an allowed human change; None for "moved" (the mirror
    already put the canonical status back, the Coordinator never sees kanban moves)."""
    kind = _EVENT_KIND.get(change["kind"])
    if kind is None:
        return None
    payload: dict = {"card_key": change["card_key"]}
    if change["kind"] == "reordered":
        payload["priority"] = change["priority"]
    elif change["kind"] == "commented":
        payload["comment"] = change["comment"]
    return Event(
        id="", ts=at, kind=kind, actor="human", project_id=project_id, run_id=None,
        payload=payload, refs=[],
    )  # fmt: skip


def result_extras(res: RunResult) -> dict:
    """Summary, artifact paths and evidence URLs of a run result, for its card."""
    links: list[str] = []
    for a in res["artifacts"]:
        for e in a.get("evidence") or []:
            url = e.get("url")
            if url and url not in links:
                links.append(url)
    return {
        "summary": res["summary"],
        "links": links[:LINKS_MAX],
        "attachments": [Path(a["path"]) for a in res["artifacts"] if a.get("path")],
    }


def card_fields(card: dict) -> dict:
    """What we send for a card. Links / attachment paths go into the body (the attachment
    upload API is unverified). Never an `assignee`, so Hermes never runs our cards."""
    extra: list[str] = []
    if card["links"]:
        extra += ["", "링크:", *(f"- {u}" for u in card["links"])]
    if card["attachments"]:
        extra += ["", "첨부:", *(f"- {p}" for p in card["attachments"])]
    body = "\n".join([card["body"], *extra]) if extra else card["body"]
    return {
        "title": card["title"],
        "body": clip(body, BODY_MAX),
        "status": card["status"],
        "priority": card["priority"],
    }
