"""Verifier: is the work done according to its done_criteria?

Deterministic checks first, and they reject without asking a model: at least one Evidence, every
evidence URL is http(s) and answers GET 200 within 10 s, every screenshot file exists, each
Evidence has a URL or a screenshot. Then one `single_shot` engine call compares the artifacts and
evidence with the done criteria (JSON `{pass, reasons}`).

The engine call runs under the Run's own worker lease (the Run is still `verifying` and holds
it), so the verifier takes no second lease and never runs unleased. There is one engine, so every
verdict is `same_engine=True`. An engine failure raises `VerifierUnavailable` (the Run is
interrupted, not failed); its usage signal goes to `ledger.observe`.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from swarm.engine.base import Engine, EngineError
from swarm.harness.base import FileOutput
from swarm.ids import new_id
from swarm.scheduler.ledger import QuotaLedger
from swarm.text import clip
from swarm.types import Artifact, Evidence, RunId, WorkOrder

log = logging.getLogger(__name__)

HTTP_TIMEOUT_S = 10.0
LLM_TIMEOUT_S = 300
ARTIFACT_TEXT_MAX = 4_000
EVIDENCE_TEXT_MAX = 1_000
ARTIFACT_TITLE_MAX = 80  # an artifact title cut from the summary or the intent
TEXT_SUFFIXES = frozenset({".md", ".txt", ".csv", ".json", ".html"})

VERDICT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "pass": {"type": "boolean"},
        "reasons": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["pass", "reasons"],
    "additionalProperties": False,
}


def collect_artifacts(
    output_dir: Path,
    intent: str,
    files: list[FileOutput],
    evidence: list[Evidence],
    summary: str,
) -> list[Artifact]:
    """The run's report (`summary.md`, written now) and every output file, all `reported`
    and each carrying the same evidence."""
    report = output_dir / "summary.md"
    report.write_text(summary + "\n", encoding="utf-8")
    title = summary.split("\n", 1)[0][:ARTIFACT_TITLE_MAX] or intent[:ARTIFACT_TITLE_MAX]
    out = [
        Artifact(
            id=new_id("art"),
            title=title,
            path=str(report),
            status="reported",
            evidence=list(evidence),
            verifier_run_id=None,
        )
    ]
    for f in files:
        out.append(
            Artifact(
                id=new_id("art"),
                title=Path(f.path).name,
                path=f.path,
                status="reported",
                evidence=list(evidence),
                verifier_run_id=None,
            )
        )
    return out


class VerifierUnavailable(RuntimeError):
    """The verifier engine failed (CLI error, timeout, quota): try again later."""


@dataclass(frozen=True)
class Verdict:
    pass_: bool
    reasons: list[str]
    same_engine: bool
    verifier_run_id: RunId | None = None


class Verifier:
    def __init__(
        self,
        engine: Engine,
        ledger: QuotaLedger,
        *,
        engine_id: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        blob_root: Path | None = None,
    ) -> None:
        self._engine = engine
        self._ledger = ledger
        self._engine_id = engine_id or engine.id
        self._transport = transport
        self._blob_root = blob_root

    async def verify(
        self, order: WorkOrder, artifacts: Sequence[Artifact], evidence: Sequence[Evidence]
    ) -> Verdict:
        vid = new_id("vrf")
        reasons = await self._deterministic(evidence)
        if reasons:
            return Verdict(pass_=False, reasons=reasons, same_engine=True, verifier_run_id=vid)
        try:
            answer = await self._engine.single_shot(
                prompt=self._prompt(order, artifacts, evidence),
                json_schema=VERDICT_SCHEMA,
                timeout_s=LLM_TIMEOUT_S,
            )
        except EngineError as e:
            if e.signal is not None:
                self._ledger.observe(engine=self._engine_id, signal=e.signal)
            raise VerifierUnavailable(str(e)) from e
        ok, why = answer.get("pass"), answer.get("reasons")
        if not isinstance(ok, bool) or not isinstance(why, list):
            raw = clip(json.dumps(answer, ensure_ascii=False), 200)
            return Verdict(
                pass_=False,
                reasons=[f"검증 응답 형식 오류: {raw}"],
                same_engine=True,
                verifier_run_id=vid,
            )
        reasons = [str(r) for r in why]
        if not ok and not reasons:
            reasons = ["완료 기준을 충족하지 않음"]
        return Verdict(pass_=ok, reasons=reasons, same_engine=True, verifier_run_id=vid)

    # --- deterministic -----------------------------------------------------------------

    def _shot_path(self, ref: str) -> Path:
        p = Path(ref)
        if not p.is_absolute() and self._blob_root is not None:
            p = self._blob_root / p
        return p

    async def _deterministic(self, evidence: Sequence[Evidence]) -> list[str]:
        if not evidence:
            return ["증거 없음 (no evidence)"]
        reasons: list[str] = []
        async with httpx.AsyncClient(
            transport=self._transport, timeout=HTTP_TIMEOUT_S, follow_redirects=True
        ) as client:
            for ev in evidence:
                url, shot = ev.get("url"), ev.get("screenshot")
                if not url and not shot:
                    reasons.append("증거에 URL도 스크린샷도 없음")
                    continue
                if url:
                    why = await self._check_url(client, url)
                    if why:
                        reasons.append(why)
                if shot and not self._shot_path(shot).is_file():
                    reasons.append(f"스크린샷 파일 없음: {shot}")
        return reasons

    @staticmethod
    async def _check_url(client: httpx.AsyncClient, url: str) -> str | None:
        if urlsplit(url).scheme not in ("http", "https"):
            return f"증거 URL이 http(s)가 아님: {url}"
        try:
            r = await client.get(url)
        except httpx.HTTPError as e:
            return f"증거 URL 접속 실패: {url} ({type(e).__name__})"
        if r.status_code != 200:
            return f"증거 URL 응답 {r.status_code}: {url}"
        return None

    # --- engine ------------------------------------------------------------------------

    @staticmethod
    def _artifact_text(a: Artifact) -> str:
        p = Path(a["path"])
        if p.suffix.lower() in TEXT_SUFFIXES and p.is_file():
            try:
                return clip(p.read_text(encoding="utf-8", errors="replace"), ARTIFACT_TEXT_MAX)
            except OSError:
                return "(읽을 수 없음)"
        return "(바이너리 파일)"

    def _prompt(
        self, order: WorkOrder, artifacts: Sequence[Artifact], evidence: Sequence[Evidence]
    ) -> str:
        crit = "\n".join(f"- {c}" for c in order["done_criteria"])
        arts = "\n\n".join(
            f"### {a['title']} ({a['path']})\n{self._artifact_text(a)}" for a in artifacts
        )
        evs = "\n".join(
            f"- url={e.get('url')} screenshot={e.get('screenshot')} "
            f"captured_at={e['captured_at'].isoformat()} "
            f"text={clip(e.get('text') or '', EVIDENCE_TEXT_MAX)}"
            for e in evidence
        )
        return (
            "너는 검증자다. 작업자가 낸 산출물과 증거가 완료 기준을 모두 충족하는지 판정하라.\n"
            "증거로 뒷받침되지 않는 주장은 충족으로 보지 않는다.\n"
            '답은 JSON {"pass": bool, "reasons": [str]} 하나.\n'
            "거절이면 reasons에 빠진 점을 적어라.\n\n"
            f"## 작업 의도\n{order['intent']}\n\n## 완료 기준\n{crit}\n\n"
            f"## 산출물\n{arts or '(없음)'}\n\n## 증거\n{evs}\n"
        )
