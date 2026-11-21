"""ProjectStore: SQLite is canonical; GOAL/POLICIES/MEMORY.md are rendered for humans and
human edits are ingested back."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol

from swarm.clock import Clock
from swarm.ids import new_id
from swarm.projects.files import ProjectFiles
from swarm.store.codec import to_json
from swarm.store.db import Database
from swarm.store.events import Emitter, EventLog
from swarm.types import (
    KR,
    Artifact,
    BacklogItem,
    BacklogItemId,
    BacklogStatus,
    ChannelRef,
    Event,
    Evidence,
    FailedAttempt,
    KrId,
    Learning,
    MemoryBrief,
    MemoryEntry,
    Policy,
    PolicyId,
    Project,
    ProjectId,
)

_SCHEMA = """
CREATE TABLE projects (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
    objective TEXT NOT NULL, strategy TEXT NOT NULL, autonomy INTEGER NOT NULL,
    context TEXT NOT NULL, channel TEXT NOT NULL UNIQUE, board TEXT NOT NULL,
    default_profile TEXT NOT NULL, default_variant_json TEXT NOT NULL,
    weight INTEGER NOT NULL, watches_json TEXT NOT NULL
);
CREATE TABLE krs (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL, id TEXT NOT NULL,
    metric TEXT NOT NULL, target REAL NOT NULL, current REAL NOT NULL, source TEXT NOT NULL,
    evidence_json TEXT, UNIQUE(project_id, id)
);
CREATE TABLE policies (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
    text TEXT NOT NULL, source_ask TEXT, active INTEGER NOT NULL, created_at TEXT NOT NULL,
    deactivated_reason TEXT
);
CREATE TABLE backlog (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
    title TEXT NOT NULL, intent TEXT NOT NULL, done_criteria_json TEXT NOT NULL,
    status TEXT NOT NULL, priority INTEGER NOT NULL, value INTEGER NOT NULL,
    note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE learnings (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
    text TEXT NOT NULL, evidence_json TEXT NOT NULL
);
CREATE TABLE failures (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
    what TEXT NOT NULL, why TEXT NOT NULL, run_id TEXT
);
CREATE TABLE artifacts (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
    title TEXT NOT NULL, path TEXT NOT NULL, status TEXT NOT NULL,
    evidence_json TEXT NOT NULL, verifier_run_id TEXT
);
CREATE TABLE global_memory (
    key TEXT PRIMARY KEY, text TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE rendered (
    path TEXT PRIMARY KEY, sha TEXT NOT NULL, seen_sha TEXT
)
"""

_SCHEMA_V2 = """
ALTER TABLE rendered ADD COLUMN baseline TEXT;
ALTER TABLE global_memory ADD COLUMN id TEXT;
ALTER TABLE global_memory ADD COLUMN active INTEGER NOT NULL DEFAULT 1;
UPDATE global_memory SET id='mem_' || lower(hex(randomblob(8))) WHERE id IS NULL
"""

_SPILL_RESERVE = 0.2  # share of each section kept for "[id] summary" spill lines
_SUMMARY_CHARS = 24


class ProjectStore(Protocol):
    def get(self, project_id: ProjectId) -> Project: ...
    def by_channel(self, channel: ChannelRef) -> Project | None: ...
    def list(self) -> list[Project]: ...
    def memory_brief(self, project_id: ProjectId, *, budget_tokens: int) -> MemoryBrief: ...
    def add_backlog(self, project_id: ProjectId, item: BacklogItem) -> BacklogItemId: ...
    def update_backlog(
        self, item_id: BacklogItemId, *, status: BacklogStatus, note: str = ""
    ) -> None: ...
    def set_backlog_priority(self, item_id: BacklogItemId, priority: int) -> None: ...
    def add_policy(self, project_id: ProjectId, policy: Policy) -> PolicyId: ...
    def record_learning(self, project_id: ProjectId, learning: Learning) -> None: ...
    def record_failure(self, project_id: ProjectId, failure: FailedAttempt) -> None: ...
    def update_kr(
        self, project_id: ProjectId, kr_id: KrId, *, value: float, evidence: Evidence
    ) -> None: ...
    def create(self, project: Project) -> ProjectId: ...
    def deactivate_policy(self, policy_id: PolicyId, *, reason: str) -> None: ...
    def attach_artifacts(self, project_id: ProjectId, artifacts: list[Artifact]) -> None: ...
    def add_global_memory(self, entry: MemoryEntry) -> None: ...
    def ingest_file_edits(self) -> list[Event]: ...
    def render_all(self) -> None: ...


def _evidence_in(raw: str) -> list[Evidence]:
    out = json.loads(raw)
    for e in out:
        e["captured_at"] = datetime.fromisoformat(e["captured_at"])
    return out


def _short(text: str) -> str:
    one = " ".join(text.split())
    return one if len(one) <= _SUMMARY_CHARS else one[:_SUMMARY_CHARS] + "…"


def find_item(projects: ProjectStore, item_id: str | None) -> tuple[Project, BacklogItem] | None:
    """The backlog item with this id (a kanban card key is the item id) and its project."""
    for project in projects.list() if item_id else []:
        for item in project["backlog"]:
            if item["id"] == item_id:
                return project, item
    return None


class RenderedRow(NamedTuple):
    sha: str
    seen_sha: str | None
    baseline: str | None


class RenderedTable:
    """The `rendered` table: for each written file, the sha and text of its last render (the
    3-way baseline) and the sha of a human edit already rejected."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def get(self, path: Path) -> RenderedRow | None:
        r = self._db.execute(
            "SELECT sha, seen_sha, baseline FROM rendered WHERE path=?", (str(path),)
        ).fetchone()
        return RenderedRow(r["sha"], r["seen_sha"], r["baseline"]) if r else None

    def record(self, path: Path, sha: str, baseline: str) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO rendered(path, sha, seen_sha, baseline) VALUES (?,?,NULL,?)",
            (str(path), sha, baseline),
        )

    def mark_seen(self, path: Path, sha: str) -> None:
        self._db.execute("UPDATE rendered SET seen_sha=? WHERE path=?", (sha, str(path)))


class SqliteProjectStore:
    def __init__(self, db: Database, log: EventLog, home: Path, clock: Clock) -> None:
        self._db = db
        self._log = log
        self._home = home
        self._clock = clock
        self._emitters = {a: Emitter(log, clock, a) for a in ("projects", "human")}
        db.migrate("projects_v1", _SCHEMA)
        db.migrate("projects_v2", _SCHEMA_V2)
        self.rendered = RenderedTable(db)
        self._files = ProjectFiles(self, home, clock)

    def render_all(self) -> None:
        self._files.render_all()

    def ingest_file_edits(self) -> list[Event]:
        return self._files.ingest_file_edits()

    # --- events -------------------------------------------------------------------

    def emit(
        self,
        kind: str,
        project_id: str | None,
        payload: dict,
        actor: Literal["projects", "human"] = "projects",
    ) -> Event:
        """Append an event as `actor` and return it with its id."""
        ev = self._emitters[actor].build(kind, payload, project_id=project_id)
        ev["id"] = self._log.append(ev)
        return ev

    # --- reads --------------------------------------------------------------------

    def _row(self, project_id: ProjectId):
        r = self._db.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        if r is None:
            raise KeyError(project_id)
        return r

    def _children(self, table: str, pid: str):
        """Rows of a child table for one project, in insertion order."""
        return self._db.execute(f"SELECT * FROM {table} WHERE project_id=? ORDER BY seq", (pid,))

    def _krs(self, pid: str) -> list[KR]:
        return [
            KR(
                id=k["id"],
                metric=k["metric"],
                target=k["target"],
                current=k["current"],
                source=k["source"],
            )
            for k in self._children("krs", pid)
        ]

    def _policies(self, pid: str) -> list[Policy]:
        return [
            Policy(
                id=p["id"],
                text=p["text"],
                active=bool(p["active"]),
                created_at=datetime.fromisoformat(p["created_at"]),
                source_ask=p["source_ask"],
            )
            for p in self._children("policies", pid)
        ]

    def _backlog(self, pid: str) -> list[BacklogItem]:
        return [
            BacklogItem(
                id=b["id"],
                title=b["title"],
                intent=b["intent"],
                done_criteria=json.loads(b["done_criteria_json"]),
                status=b["status"],
                priority=b["priority"],
                value=b["value"],
            )
            for b in self._children("backlog", pid)
        ]

    def _learnings(self, pid: str) -> list[Learning]:
        return [
            Learning(text=x["text"], evidence=_evidence_in(x["evidence_json"]))
            for x in self._children("learnings", pid)
        ]

    def _failures(self, pid: str) -> list[FailedAttempt]:
        return [
            FailedAttempt(what=x["what"], why=x["why"], run_id=x["run_id"])
            for x in self._children("failures", pid)
        ]

    def _artifacts(self, pid: str) -> list[dict]:
        return [
            {"id": a["id"], "title": a["title"], "path": a["path"]}
            for a in self._children("artifacts", pid)
        ]

    def _assemble(self, r) -> Project:
        pid = r["id"]
        return Project(
            id=pid,
            slug=r["slug"],
            name=r["name"],
            goal={"objective": r["objective"], "key_results": self._krs(pid)},
            strategy=r["strategy"],
            policies=self._policies(pid),
            autonomy=r["autonomy"],
            memory={
                "context": r["context"],
                "learnings": self._learnings(pid),
                "failed_attempts": self._failures(pid),
            },
            backlog=self._backlog(pid),
            artifacts=self._artifacts(pid),
            channel=r["channel"],
            board=r["board"],
            default_profile=r["default_profile"],
            default_variant=json.loads(r["default_variant_json"]),
            weight=r["weight"],
            watches=json.loads(r["watches_json"]),
        )

    def get(self, project_id: ProjectId) -> Project:
        return self._assemble(self._row(project_id))

    def by_channel(self, channel: ChannelRef) -> Project | None:
        r = self._db.execute("SELECT * FROM projects WHERE channel=?", (channel,)).fetchone()
        return self._assemble(r) if r else None

    def list(self) -> list[Project]:
        return [self._assemble(r) for r in self._db.execute("SELECT * FROM projects ORDER BY seq")]

    # --- writes -------------------------------------------------------------------

    def create(self, project: Project) -> ProjectId:
        pid = project.get("id") or new_id("prj")
        g = project["goal"]
        mem = project["memory"]
        with self._db.transaction():
            self._db.execute(
                "INSERT INTO projects(id, slug, name, objective, strategy, autonomy, context,"
                " channel, board, default_profile, default_variant_json, weight, watches_json)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    pid, project["slug"], project["name"], g["objective"], project["strategy"],
                    project["autonomy"], mem["context"], project["channel"], project["board"],
                    project["default_profile"], to_json(project["default_variant"]),
                    project["weight"], to_json(project["watches"]),
                ),
            )  # fmt: skip
            for k in g["key_results"]:
                self._db.execute(
                    "INSERT INTO krs(project_id, id, metric, target, current, source)"
                    " VALUES (?,?,?,?,?,?)",
                    (pid, k["id"], k["metric"], k["target"], k["current"], k["source"]),
                )
            for p in project["policies"]:
                self._insert_policy(pid, p)
            for b in project["backlog"]:
                self._insert_backlog(pid, b)
        self.emit("project.created", pid, {"slug": project["slug"], "name": project["name"]})
        self._files.render_project(pid)
        self._files.render_global()
        return pid

    def _insert_policy(self, pid: str, policy: Policy) -> str:
        polid = policy.get("id") or new_id("pol")
        created = policy.get("created_at") or self._clock.now()
        self._db.execute(
            "INSERT INTO policies(id, project_id, text, source_ask, active, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (polid, pid, policy["text"], policy.get("source_ask"), int(policy.get("active", True)),
             created.isoformat()),
        )  # fmt: skip
        return polid

    def _insert_backlog(self, pid: str, item: BacklogItem) -> str:
        bid = item.get("id") or new_id("bl")
        self._db.execute(
            "INSERT INTO backlog(id, project_id, title, intent, done_criteria_json, status,"
            " priority, value) VALUES (?,?,?,?,?,?,?,?)",
            (bid, pid, item["title"], item.get("intent", ""),
             to_json(item.get("done_criteria", [])), item.get("status", "triage"),
             item.get("priority", 0), item.get("value", 3)),
        )  # fmt: skip
        return bid

    def add_policy(self, project_id: ProjectId, policy: Policy) -> PolicyId:
        self._row(project_id)
        polid = self._insert_policy(project_id, policy)
        self.emit(
            "project.policy_added",
            project_id,
            {"policy_id": polid, "text": policy["text"], "source_ask": policy.get("source_ask")},
        )
        self._files.render_project(project_id)
        return polid

    def deactivate_policy(self, policy_id: PolicyId, *, reason: str) -> None:
        self._deactivate_policy(policy_id, reason=reason)

    def _deactivate_policy(
        self, policy_id: PolicyId, *, reason: str, by: str = "api"
    ) -> list[Event]:
        r = self._db.execute("SELECT project_id FROM policies WHERE id=?", (policy_id,)).fetchone()
        if r is None:
            raise KeyError(policy_id)
        self._db.execute(
            "UPDATE policies SET active=0, deactivated_reason=? WHERE id=?", (reason, policy_id)
        )  # row is kept: policies are add/deactivate only
        ev = self.emit(
            "project.policy_deactivated",
            r["project_id"],
            {"policy_id": policy_id, "reason": reason, "by": by},
        )
        self._files.render_project(r["project_id"])
        return [ev]

    def add_backlog(self, project_id: ProjectId, item: BacklogItem) -> BacklogItemId:
        self._row(project_id)
        bid = self._insert_backlog(project_id, item)
        self.emit("project.backlog_added", project_id, {"item_id": bid, "title": item["title"]})
        return bid

    def update_backlog(
        self, item_id: BacklogItemId, *, status: BacklogStatus, note: str = ""
    ) -> None:
        r = self._db.execute(
            "SELECT project_id, status FROM backlog WHERE id=?", (item_id,)
        ).fetchone()
        if r is None:
            raise KeyError(item_id)
        self._db.execute("UPDATE backlog SET status=?, note=? WHERE id=?", (status, note, item_id))
        self.emit(
            "project.backlog_updated",
            r["project_id"],
            {"item_id": item_id, "from": r["status"], "status": status, "note": note},
        )

    def set_backlog_priority(self, item_id: BacklogItemId, priority: int) -> None:
        """Human reorder (kanban): only the priority changes."""
        r = self._db.execute(
            "SELECT project_id, priority FROM backlog WHERE id=?", (item_id,)
        ).fetchone()
        if r is None:
            raise KeyError(item_id)
        self._db.execute("UPDATE backlog SET priority=? WHERE id=?", (priority, item_id))
        self.emit(
            "project.backlog_reprioritized",
            r["project_id"],
            {"item_id": item_id, "from": r["priority"], "priority": priority},
        )

    def record_learning(self, project_id: ProjectId, learning: Learning) -> None:
        self._row(project_id)
        lid = new_id("lrn")
        self._db.execute(
            "INSERT INTO learnings(id, project_id, text, evidence_json) VALUES (?,?,?,?)",
            (lid, project_id, learning["text"], to_json(learning["evidence"])),
        )
        self.emit(
            "project.learning_recorded", project_id, {"learning_id": lid, "text": learning["text"]}
        )
        self._files.render_project(project_id)

    def record_failure(self, project_id: ProjectId, failure: FailedAttempt) -> None:
        self._row(project_id)
        fid = new_id("fail")
        self._db.execute(
            "INSERT INTO failures(id, project_id, what, why, run_id) VALUES (?,?,?,?,?)",
            (fid, project_id, failure["what"], failure["why"], failure["run_id"]),
        )
        self.emit(
            "project.failure_recorded",
            project_id,
            {"failure_id": fid, "what": failure["what"], "why": failure["why"]},
        )
        self._files.render_project(project_id)

    def update_kr(
        self, project_id: ProjectId, kr_id: KrId, *, value: float, evidence: Evidence
    ) -> None:
        cur = self._db.execute(
            "UPDATE krs SET current=?, evidence_json=? WHERE project_id=? AND id=?",
            (value, to_json(evidence), project_id, kr_id),
        )
        if cur.rowcount == 0:
            raise KeyError(kr_id)
        self.emit(
            "project.kr_updated",
            project_id,
            {"kr_id": kr_id, "value": value, "evidence": json.loads(to_json(evidence))},
        )
        self._files.render_project(project_id)

    def attach_artifacts(self, project_id: ProjectId, artifacts: list[Artifact]) -> None:
        self._row(project_id)
        for a in artifacts:
            self._db.execute(
                "INSERT OR REPLACE INTO artifacts(id, project_id, title, path, status,"
                " evidence_json, verifier_run_id) VALUES (?,?,?,?,?,?,?)",
                (a["id"], project_id, a["title"], a["path"], a["status"],
                 to_json(a["evidence"]), a["verifier_run_id"]),
            )  # fmt: skip
        self.emit(
            "project.artifacts_attached", project_id, {"artifact_ids": [a["id"] for a in artifacts]}
        )

    def add_global_memory(self, entry: MemoryEntry) -> None:
        with self._db.transaction():
            row = self._db.execute(
                "SELECT id FROM global_memory WHERE key=?", (entry["key"],)
            ).fetchone()
            mid = row["id"] if row else new_id("mem")
            self._db.execute(
                "INSERT OR REPLACE INTO global_memory(key, text, updated_at, id, active)"
                " VALUES (?,?,?,?,1)",
                (entry["key"], entry["text"], entry["updated_at"].isoformat(), mid),
            )
        self.emit("project.global_memory_set", None, {"key": entry["key"], "text": entry["text"]})
        self._files.render_global()

    # --- human-edit ingest writes -------------------------------------------------
    # Narrow writes for `ProjectFiles.ingest_file_edits`. Unlike the public mutators they emit
    # no event and render nothing: ingest emits its own `by="file"` events and re-renders once
    # at the end.

    def transaction(self):
        """The database's re-entrant transaction (ingest applies one file's edit atomically)."""
        return self._db.transaction()

    def project_ids(self) -> list[str]:
        return [r["id"] for r in self._db.execute("SELECT id FROM projects ORDER BY seq")]

    def global_memory_texts(self) -> list[tuple[str, str]]:
        """(id, text) of every active global entry, in rendered order."""
        return [(r["id"], r["text"]) for r in self._global_rows()]

    def global_memory_active(self) -> dict[str, bool]:
        """Every global entry id (active or not) -> whether it is active."""
        return {
            r["id"]: bool(r["active"])
            for r in self._db.execute("SELECT id, active FROM global_memory")
        }

    def set_objective(self, project_id: str, objective: str) -> None:
        self._db.execute("UPDATE projects SET objective=? WHERE id=?", (objective, project_id))

    def set_context(self, project_id: str, context: str) -> None:
        self._db.execute("UPDATE projects SET context=? WHERE id=?", (context, project_id))

    def set_kr_field(self, project_id: str, kr_id: str, field: str, value: Any) -> None:
        assert field in ("metric", "target", "current")
        self._db.execute(
            f"UPDATE krs SET {field}=? WHERE project_id=? AND id=?", (value, project_id, kr_id)
        )

    def retire_policy(self, policy_id: str, reason: str) -> None:
        self._db.execute(
            "UPDATE policies SET active=0, deactivated_reason=? WHERE id=?", (reason, policy_id)
        )

    def edit_global_entry(self, entry_id: str, text: str, at: datetime) -> None:
        self._db.execute(
            "UPDATE global_memory SET text=?, updated_at=? WHERE id=?",
            (text, at.isoformat(), entry_id),
        )

    def retire_global_entry(self, entry_id: str) -> None:
        self._db.execute("UPDATE global_memory SET active=0 WHERE id=?", (entry_id,))

    def add_global_entry(self, entry_id: str, text: str, at: datetime) -> None:
        """A human-added line: its id doubles as its key."""
        self._db.execute(
            "INSERT INTO global_memory(key, text, updated_at, id, active) VALUES (?,?,?,?,1)",
            (entry_id, text, at.isoformat(), entry_id),
        )

    # --- memory brief -------------------------------------------------------------

    def _global_rows(self):
        return self._db.execute(
            "SELECT * FROM global_memory WHERE active=1 ORDER BY updated_at DESC, key"
        ).fetchall()

    @staticmethod
    def _fit(
        title: str, items: list[tuple[str, str, str]], budget_chars: int
    ) -> tuple[list[str], list[str]]:
        """Inline & Spill. items = (ref_id, full line, summary). Returns (lines, spilled ids)."""
        if budget_chars <= len(title) + 1:
            return [], [i[0] for i in items]
        lines = [title]
        used = len(title) + 1
        inline_cap = budget_chars - int(budget_chars * _SPILL_RESERVE)
        spilled: list[str] = []
        for ref, line, summary in items:
            if not spilled and used + len(line) + 1 <= inline_cap:
                lines.append(line)
                used += len(line) + 1
                continue
            spilled.append(ref)
            sp = f"[{ref}] {summary}"
            if used + len(sp) + 1 <= budget_chars:
                lines.append(sp)
                used += len(sp) + 1
        if len(lines) == 1:
            return [], spilled
        return lines, spilled

    def memory_brief(self, project_id: ProjectId, *, budget_tokens: int) -> MemoryBrief:
        p = self.get(project_id)
        total = budget_tokens * 3
        glob = [
            (r["id"], f"- {r['key']}: {_short_full(r['text'])}", _short(r["text"]))
            for r in self._global_rows()
        ]
        proj: list[tuple[str, str, str]] = []
        if p["memory"]["context"].strip():
            proj.append(
                (
                    f"ctx:{p['slug']}",
                    "맥락: " + _short_full(p["memory"]["context"]),
                    _short(p["memory"]["context"]),
                )
            )
        lrows = self._db.execute(
            "SELECT id, text FROM learnings WHERE project_id=? ORDER BY seq DESC", (project_id,)
        ).fetchall()
        proj += [(r["id"], "- " + _short_full(r["text"]), _short(r["text"])) for r in lrows]
        frows = self._db.execute(
            "SELECT id, what, why FROM failures WHERE project_id=? ORDER BY seq DESC", (project_id,)
        ).fetchall()
        fails = [
            (r["id"], f"- {_short_full(r['what'])} -> {_short_full(r['why'])}", _short(r["what"]))
            for r in frows
        ]
        sections = [
            ("## 전역 기억", glob, total * 25 // 100),
            ("## 프로젝트 기억", proj, total * 50 // 100),
            ("## 실패한 시도", fails, total * 25 // 100),
        ]
        blocks: list[str] = []
        refs: list[str] = []
        for title, items, cap in sections:
            lines, spilled = self._fit(title, items, cap - 1)  # -1: block separator newline
            refs += spilled
            if lines:
                blocks.append("\n".join(lines))
        text = "\n".join(blocks)
        while len(text) // 3 > budget_tokens and text:  # safety net for rounding
            text = text.rsplit("\n", 1)[0] if "\n" in text else ""
        return MemoryBrief(text=text, refs=refs, tokens=len(text) // 3)


def _short_full(text: str) -> str:
    return " ".join(text.split())
