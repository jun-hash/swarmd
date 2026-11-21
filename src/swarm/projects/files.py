"""Render projects to GOAL/POLICIES/MEMORY.md and ingest human edits back.

This module parses, renders and decides; the store owns every SQL statement. Ingest writes
through the store's narrow `set_*` / `retire_*` / `*_global_entry` methods and the `rendered`
table, not through the public mutators (`add_policy`, `deactivate_policy`, `update_kr`, ...).
Those emit `by="api"` events and re-render the project on every call; ingest must emit its own
`by="file"` events as the human actor and render once, after the whole pass.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from swarm.clock import Clock
from swarm.ids import new_id
from swarm.projects import render as R
from swarm.types import Event

if TYPE_CHECKING:
    from swarm.projects.store import SqliteProjectStore


class ProjectFiles:
    def __init__(
        self,
        store: SqliteProjectStore,
        home: Path,
        clock: Clock,
    ) -> None:
        self._store = store
        self._home = home
        self._clock = clock

    def _write_rendered(self, path: Path, content: str, *, force: bool = False) -> None:
        """Write `content` unless a human edit is pending (file differs from last render).
        The rendered content is stored as the baseline for 3-way ingestion."""
        row = self._store.rendered.get(path)
        if not force and row and path.exists() and R.sha(path.read_text()) != row.sha:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        self._store.rendered.record(path, R.sha(content), content)

    def _project_files(self, project_id: str) -> dict[str, tuple[Path, str]]:
        p = self._store.get(project_id)
        d = self._home / "projects" / p["slug"]
        mem = p["memory"]
        return {
            "GOAL": (
                d / "GOAL.md",
                R.render_goal(p["goal"]["objective"], p["goal"]["key_results"]),
            ),
            "POLICIES": (d / "POLICIES.md", R.render_policies(p["policies"])),
            "MEMORY": (
                d / "MEMORY.md",
                R.render_project_memory(mem["context"], mem["learnings"], mem["failed_attempts"]),
            ),
        }

    def render_project(self, project_id: str) -> None:
        for path, content in self._project_files(project_id).values():
            self._write_rendered(path, content)

    def _global_file(self) -> tuple[Path, str]:
        return self._home / "MEMORY.md", R.render_global_memory(self._store.global_memory_texts())

    def render_global(self, *, force: bool = False) -> None:
        path, content = self._global_file()
        self._write_rendered(path, content, force=force)

    def render_all(self) -> None:
        for pid in self._store.project_ids():
            self.render_project(pid)
        self.render_global()

    def _pending(self, path: Path) -> tuple[str, str] | None:
        """(current text, baseline) if the human changed `path` since the last render and the
        change has not been rejected already; else None."""
        if not path.exists():
            return None
        row = self._store.rendered.get(path)
        if row is None or row.baseline is None:
            return None
        text = path.read_text()
        if R.sha(text) in (row.sha, row.seen_sha):
            return None
        return text, row.baseline

    def _reject(
        self, path: Path, text: str, kind: str, pid: str | None, name: str, err: str
    ) -> Event:
        self._store.rendered.mark_seen(path, R.sha(text))
        return self._store.emit(kind, pid, {"file": name, "error": err, "by": "file"})

    def ingest_file_edits(self) -> list[Event]:
        """3-way: apply only what the human changed relative to the last rendered baseline,
        then re-render from canonical state."""
        events: list[Event] = []
        for pid in self._store.project_ids():
            accepted: set[str] = set()
            for name, (path, _) in self._project_files(pid).items():
                pending = self._pending(path)
                if pending is None:
                    continue
                text, baseline = pending
                try:
                    with self._store.transaction():
                        specs = self._apply_edit(pid, name, text, baseline)
                except R.ParseError as e:
                    events.append(
                        self._reject(
                            path, text, "project.file_edit_rejected", pid, f"{name}.md", str(e)
                        )
                    )
                    continue
                accepted.add(name)
                events += [self._store.emit(k, pid, pl, actor="human") for k, pl in specs]
            for name, (path, content) in self._project_files(pid).items():
                self._write_rendered(path, content, force=name in accepted)
        path, _ = self._global_file()
        pending = self._pending(path)
        if pending is not None:
            text, baseline = pending
            try:
                with self._store.transaction():
                    specs = self._apply_global(text, baseline)
            except R.ParseError as e:
                events.append(
                    self._reject(path, text, "memory.file_edit_rejected", None, "MEMORY.md", str(e))
                )
            else:
                events += [self._store.emit(k, None, pl, actor="human") for k, pl in specs]
                self.render_global(force=True)
        self.render_global()
        return events

    def _apply_edit(self, pid: str, name: str, text: str, baseline: str) -> list[tuple[str, dict]]:
        """DB mutations only (runs inside a transaction); returns (kind, payload) events."""
        if name == "GOAL":
            return self._apply_goal(pid, text, baseline)
        if name == "POLICIES":
            return self._apply_policies(pid, text, baseline)
        return self._apply_memory(pid, text, baseline)

    def _apply_goal(self, pid: str, text: str, baseline: str) -> list[tuple[str, dict]]:
        objective, rows = R.parse_goal(text)
        b_obj, b_rows = R.parse_goal(baseline)
        if [x[0] for x in rows] != [x[0] for x in b_rows]:
            raise R.ParseError("GOAL.md: KR ids must match existing KRs (no add/remove/reorder)")
        changes: dict[str, Any] = {}
        if objective != b_obj:
            changes["objective"] = objective
        kr_changes = []
        canon = {k["id"]: k for k in self._store.get(pid)["goal"]["key_results"]}
        for new, old in zip(rows, b_rows, strict=True):
            if new != old:  # only fields the human changed vs baseline
                kr_changes.append(
                    {
                        "id": new[0],
                        **{
                            f: v
                            for f, v, o in zip(
                                ("metric", "target", "current"), new[1:], old[1:], strict=True
                            )
                            if v != o
                        },
                    }
                )
        if kr_changes:
            changes["key_results"] = kr_changes
        if not changes:
            return []
        if "objective" in changes:
            self._store.set_objective(pid, objective)
        for c in kr_changes:
            if c["id"] not in canon:
                raise R.ParseError(f"GOAL.md: unknown KR {c['id']}")
            for f in ("metric", "target", "current"):
                if f in c:
                    self._store.set_kr_field(pid, c["id"], f, c[f])
        return [("project.goal_edited", {"by": "file", **changes})]

    def _apply_policies(self, pid: str, text: str, baseline: str) -> list[tuple[str, dict]]:
        parsed = R.parse_policies(text)
        base = R.parse_policies(baseline)
        canon = {x["id"]: x for x in self._store.get(pid)["policies"]}
        for polid, ptext in parsed.items():
            if polid not in canon:
                raise R.ParseError(f"POLICIES.md: unknown policy id {polid} (add via the store)")
            if polid in base and base[polid] != ptext or polid not in base:
                raise R.ParseError(f"POLICIES.md: policy {polid} is immutable; delete the line")
        specs = []
        for polid in base:  # only policies the human saw and removed
            if polid not in parsed and canon[polid]["active"]:
                reason = "사람이 파일에서 삭제"
                self._store.retire_policy(polid, reason)
                specs.append(
                    (
                        "project.policy_deactivated",
                        {"policy_id": polid, "reason": reason, "by": "file"},
                    )
                )
        return specs

    def _apply_memory(self, pid: str, text: str, baseline: str) -> list[tuple[str, dict]]:
        context, lists = R.parse_project_memory(text)
        b_context, b_lists = R.parse_project_memory(baseline)
        specs: list[tuple[str, dict]] = []
        if context != b_context:
            self._store.set_context(pid, context)
            specs.append(("project.memory_edited", {"by": "file", "context": context}))
        if lists != b_lists:
            specs.append(
                (
                    "project.file_edit_rejected",
                    {
                        "file": "MEMORY.md",
                        "error": "learnings/failures are append-only",
                        "by": "file",
                    },
                )
            )
        return specs

    def _apply_global(self, text: str, baseline: str) -> list[tuple[str, dict]]:
        known, new = R.parse_global_memory(text)
        base, _ = R.parse_global_memory(baseline)
        canon = self._store.global_memory_active()
        for mid in known:
            if mid not in canon:
                raise R.ParseError(f"MEMORY.md: unknown entry id {mid}")
        specs: list[tuple[str, dict]] = []
        now = self._clock.now()
        for mid, mtext in known.items():
            if mid in base and base[mid] != mtext and canon[mid]:
                self._store.edit_global_entry(mid, mtext, now)
                specs.append(("memory.entry_updated", {"id": mid, "text": mtext, "by": "file"}))
        for mid in base:
            if mid not in known and canon[mid]:
                self._store.retire_global_entry(mid)
                specs.append(("memory.entry_deactivated", {"id": mid, "by": "file"}))
        for ntext in new:
            mid = new_id("mem")
            self._store.add_global_entry(mid, ntext, now)
            specs.append(("memory.entry_added", {"id": mid, "text": ntext, "by": "file"}))
        return specs
