"""Improver: self-improvement when there is nothing better to do, and triage.

An improve turn runs the oldest task in the rotation that has something to do, then moves
it to the back: replay the latest failed order once with its failure as a lesson, or
consolidate a project's learnings. Meta work stays under META_CAP of recent dispatches.
A discover run's output becomes triage candidates, valued by the Brain, never auto-ready.
"""

import contextlib
import json
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel

from swarm.aio import TaskSet
from swarm.clock import Clock
from swarm.coordinator.agenda import IMPROVE_ROTATION
from swarm.coordinator.dispatch import Dispatcher
from swarm.coordinator.messages import BACKLOG_TITLE_MAX, Candidates
from swarm.coordinator.records import CoordinatorRecords
from swarm.ids import new_id
from swarm.ports import Brain, BrainError
from swarm.projects.store import ProjectStore, find_item
from swarm.store.events import Emitter, EventLog
from swarm.types import BacklogItem, Event, MemoryEntry, RunResult, WorkOrder
from swarm.workers.contract import INTENT_MAX, order_from_json

META_CAP = 0.15  # meta work (self documents) at most 15% of recent dispatches
META_TASKS = frozenset({"memory_consolidate"})
HISTORY = timedelta(days=7)  # rolling window for the meta share and last advance/discover
CONSOLIDATE_RETRY = timedelta(minutes=5)  # after a failed consolidation (no spin on the Brain)
TRIAGE_MAX = 10
ARTIFACT_READ_MAX = 8_000  # one artifact's text in the triage prompt
TRIAGE_PROMPT_MAX = 16_000
LEARNING_MAX = 2_000


class Consolidation(BaseModel):
    """memory_consolidate (Brain purpose="report"): one deduplicated learnings summary."""

    text: str


class Improver:
    def __init__(
        self,
        *,
        records: CoordinatorRecords,
        dispatch: Dispatcher,
        projects: ProjectStore,
        brain: Brain,
        log: EventLog,
        emit: Emitter,
        tasks: TaskSet,
        clock: Clock,
    ) -> None:
        self._dispatch = dispatch
        self._kv = records.kv
        self._projects = projects
        self._brain = brain
        self._log = log
        self._emit = emit
        self._tasks = tasks
        self._clock = clock

    def history(self, now: datetime) -> list[Event]:
        return list(self._log.query(kinds=["agenda.dispatched"], since=now - HISTORY))

    def rotation(self, history: list[Event]) -> list[str]:
        rot = list(self._kv.get_json("improve_rotation", IMPROVE_ROTATION))
        meta = sum(1 for e in history if e["payload"].get("improve_task") in META_TASKS)
        if history and meta / len(history) >= META_CAP:
            rot = [t for t in rot if t not in META_TASKS]
        return rot

    def _rotate(self, task: str) -> None:
        rot = [t for t in self._kv.get_json("improve_rotation", IMPROVE_ROTATION) if t != task]
        self._kv.set_json("improve_rotation", [*rot, task])

    async def triage(self, order: WorkOrder, res: RunResult) -> None:
        """Discover output -> triage candidates (value 1~5 by the Brain, never auto-ready)."""
        parts = [res["summary"]]
        for a in res["artifacts"]:
            with contextlib.suppress(OSError, UnicodeDecodeError):
                parts.append(Path(a["path"]).read_text()[:ARTIFACT_READ_MAX])
        goal = self._projects.get(order["project_id"])["goal"]
        prompt = "\n".join(
            [
                "발굴 작업의 결과에서 목표에 도움이 될 할 일 후보를 뽑아라 (최대 10개).",
                "value는 1~5 (목표 KR에 얼마나 도움이 되나), reason은 한 줄 근거.",
                f"목표: {goal['objective']}",
                *[
                    f"- {k['metric']}: {k['current']:g} / {k['target']:g}"
                    for k in goal["key_results"]
                ],
                "",
                "결과:",
                "\n\n".join(parts)[:TRIAGE_PROMPT_MAX],
            ]
        )
        try:
            found = await self._brain.decide(
                purpose="value_estimate", prompt=prompt, schema=Candidates
            )
        except BrainError as e:
            self._emit("coordinator.triage_failed", project_id=order["project_id"], error=str(e))
            return
        for c in found.items[:TRIAGE_MAX]:
            self._projects.add_backlog(
                order["project_id"],
                BacklogItem(
                    id="", title=c.title[:BACKLOG_TITLE_MAX], intent=c.intent,
                    done_criteria=[c.title], status="triage", priority=0, value=c.value,
                ),
            )  # fmt: skip
        self._emit("coordinator.triaged", project_id=order["project_id"], count=len(found.items))

    async def improve(self, now: datetime) -> None:
        """The oldest improve task that has something to do runs, then goes to the back."""
        for task in self.rotation(self.history(now)):
            ran = self._replay_failed(now) if task == "replay_failed" else self._consolidate()
            if ran:
                self._rotate(task)
                return

    def _replay_failed(self, now: datetime) -> bool:
        """Re-run the latest failed (non-replay) order once, same Variant, failure injected."""
        replayed = {e["payload"].get("replay_of") for e in self.history(now)}
        fails = [
            e
            for e in self._log.query(kinds=["coordinator.run_failed"], since=now - HISTORY)
            if e["payload"].get("origin") != "improve" and e["run_id"] not in replayed
        ]
        for e in reversed(fails):
            p = e["payload"]
            order = order_from_json(json.dumps(p["order"]))
            item = order.get("backlog_item_id")
            found = find_item(self._projects, item) if item else None
            if item and (found is None or found[1]["status"] in ("dropped", "archived", "done")):
                continue  # the human dropped it, or it got done meanwhile
            f = p["failure"]
            lesson = f"이전 시도 실패 (run {e['run_id']}): {f['what']} -> {f['why']}"
            intent = f"{order['intent']}\n\n{lesson}\n같은 방법을 되풀이하지 말 것."
            retry = {**order, "id": new_id("wo"), "intent": intent[:INTENT_MAX]}
            order = WorkOrder(**retry)  # type: ignore[typeddict-item]
            return self._dispatch.launch(
                order, p["variant"], origin="improve", purpose="improve",
                improve_task="replay_failed", replay_of=e["run_id"],
            )  # fmt: skip
        return False

    def _consolidate(self) -> bool:
        after = self._kv.get_json("consolidate_after")
        if after and datetime.fromisoformat(after) > self._clock.now():
            return False  # the last one failed (Brain lease / error): not again so soon
        done: dict = self._kv.get_json("consolidated", {})
        cands = [
            (len(p["memory"]["learnings"]) - done.get(p["id"], 0), p)
            for p in self._projects.list()
            if len(p["memory"]["learnings"]) >= 2
        ]
        cands = [c for c in cands if c[0] > 0]
        if not cands:
            return False
        _, p = max(cands, key=lambda c: c[0])
        self._emit(
            "agenda.dispatched", project_id=p["id"], kind="improve",
            improve_task="memory_consolidate", origin="improve", order_id=None,
        )  # fmt: skip
        # marked before the Brain call: a concurrent tick must not dispatch it again
        self._kv.set_json("consolidated", {**done, p["id"]: len(p["memory"]["learnings"])})
        self._tasks.spawn(self._consolidate_run(p, done.get(p["id"], 0)), f"consolidate:{p['id']}")
        return True

    async def _consolidate_run(self, project, previous: int) -> None:
        learnings = project["memory"]["learnings"]
        prompt = "\n".join(
            [
                "프로젝트 배움 목록에서 중복을 합쳐 짧은 요약 하나로 정리하라.",
                "새 사실을 지어내지 말고, 서로 다른 배움은 각각 한 줄로 남긴다.",
                *[f"- {x['text']}" for x in learnings],
            ]
        )
        try:
            r = await self._brain.decide(purpose="report", prompt=prompt, schema=Consolidation)
            self._projects.add_global_memory(
                MemoryEntry(key=f"learnings:{project['slug']}", text=r.text[:LEARNING_MAX],
                            updated_at=self._clock.now())
            )  # fmt: skip
        except Exception as e:  # noqa: BLE001 - BrainError, a store error, anything
            self._emit("coordinator.consolidate_failed", project_id=project["id"], error=repr(e))
            done = self._kv.get_json("consolidated", {})
            self._kv.set_json("consolidated", {**done, project["id"]: previous})  # retry later
            self._kv.set_json(
                "consolidate_after", (self._clock.now() + CONSOLIDATE_RETRY).isoformat()
            )
