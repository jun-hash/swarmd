"""Worker prompt template for the own harness. Kept apart from the policies so the wording can be
iterated without touching session logic. `render_worker_prompt` also enforces the size cap: every
variable section gets a fixed share of `max_chars`, so no single input can crowd out the rules."""

from swarm.types import HarnessTask

RULES = (
    "되돌릴 수 없는 행동 전에는 ask_human(irreversible=true)으로 승인을 받을 것. "
    "제출·게시·결제·삭제·메시지 발송은 승인 없이 하지 않는다.",
    "주장마다 record_evidence로 증거(URL, 주장, 스크린샷)를 남길 것. "
    "증거 없는 주장은 보고하지 않는다.",
    "새 탭을 열지 말 것 (현재 탭에서만 이동). 다른 페이지는 browser_goto로 연다.",
    "도구 결과에 '승인이 필요해 차단됐습니다'가 나오면 우회하지 말고 "
    "ask_human으로 승인을 요청할 것.",
    "ask_human 결과가 '질문을 사람에게 보냈습니다'로 시작하면 즉시 턴을 끝낼 것. "
    "답은 다음 입력으로 온다.",
    "browser_snapshot의 @e 번호는 다음 snapshot까지만 유효하다. 페이지가 바뀌면 다시 snapshot한다.",
)

MODE_TEXT = {
    "guarded": "모드: guarded — 외부에 영향을 주는 요청은 가드가 막고, 승인을 받으면 통과한다.",
    "dry_run": "모드: dry_run — 외부에 영향을 주는 제출·게시·결제는 절대 실행하지 말고, "
    "무엇을 어떻게 실행할지만 보고한다.",
}

HEADER = (
    "너는 swarm의 브라우저 작업자다. 아래 의도를 이루기 위해 swarmbrowser 도구로 "
    "이미 열린 브라우저 탭 하나를 조작한다. 의도를 우선하고, 완료 기준을 모두 채우면 끝낸다."
)

FINISH = (
    "## 끝내기\n"
    "완료 기준을 모두 채웠으면 결과를 짧게 요약하고(근거 URL 포함) 마지막 메시지로 남긴 뒤 끝낸다. "
    "막혀서 더 나아갈 수 없으면 무엇을 시도했고 왜 막혔는지 요약하고 끝낸다."
)

# Browser MCP refs (@e<n>) do not survive a new engine segment, hence the snapshot line.
RECHECK = "먼저 browser_snapshot으로 현재 화면을 다시 확인한 뒤 이어가라."
STEER_PROMPT = (
    "사람 지시: {text}\n"
    "이 지시를 반영해 하던 작업을 이어가라. 의도·완료 기준·규칙은 그대로다.\n" + RECHECK
)
STEER_MAX_CHARS = 2_000  # joined steer text; the newest instructions are kept
CONTINUE_TEXT = "## 시작\n현재 탭에서 이어서 진행한다. 처음 URL로 되돌아가지 말 것."

# Share of max_chars per variable section (sum < 1; the rest is fixed text).
SHARES = {
    "instruction": 0.10,
    "done_criteria": 0.05,
    "memory_brief": 0.30,
    "policies": 0.10,
    "recent_failures": 0.10,
    "notes": 0.20,
    "open_tabs": 0.05,
}
CUT = "…(잘림)"


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(limit - len(CUT), 0)] + CUT


def _bullets(items: list[str], limit: int, *, keep: str = "first") -> str:
    """Bulleted list within `limit` chars. keep="first" keeps the head, "last" the newest."""
    seq = list(items) if keep == "first" else list(reversed(items))
    out: list[str] = []
    used = 0
    for item in seq:
        line = "- " + _clip(item.strip(), max(limit // 4, 80))
        if used + len(line) + 1 > limit:
            break
        out.append(line)
        used += len(line) + 1
    if keep == "last":
        out.reverse()
    return "\n".join(out)


def render_worker_prompt(
    task: HarnessTask, *, notes: list[str], open_tabs: list[str], max_chars: int
) -> str:
    def cap(name: str) -> int:
        return int(max_chars * SHARES[name])

    intent = _clip(task["instruction"], cap("instruction"))
    parts = [HEADER, "## 의도 (목표 · 이유 · 제약)\n" + intent]
    parts.append("## 완료 기준\n" + _bullets(task["done_criteria"], cap("done_criteria")))
    first_session = not notes and not open_tabs
    if not first_session:
        parts.append(CONTINUE_TEXT)
    elif task["start_url"]:
        parts.append(f"## 시작\n먼저 browser_goto로 {_clip(task['start_url'], 2000)} 을 연다.")
    if task["memory_brief"].strip():
        parts.append(
            "## 기억 (반드시 참고)\n" + _clip(task["memory_brief"].strip(), cap("memory_brief"))
        )
    if task["policies"]:
        parts.append("## 정책 (사람이 정한 규칙)\n" + _bullets(task["policies"], cap("policies")))
    failures = task.get("recent_failures") or []
    if failures:
        parts.append(
            "## 최근 실패한 시도 (같은 방법을 되풀이하지 말 것)\n"
            + _bullets(failures, cap("recent_failures"))
        )
    if notes:
        parts.append(
            "## 진행 노트 (이전 세션에서 이어짐 — 이미 한 일은 반복하지 말 것)\n"
            + _bullets(notes, cap("notes"), keep="last")
        )
    if open_tabs:
        parts.append("## 열린 탭 (현재 탭)\n" + _bullets(open_tabs, cap("open_tabs"), keep="last"))
    parts.append("## 규칙\n" + "\n".join("- " + r for r in RULES))
    parts.append(MODE_TEXT[task["action_mode"]])
    parts.append(FINISH)
    return "\n\n".join(parts)


def render_steer_prompt(text: str) -> str:
    if len(text) > STEER_MAX_CHARS:
        text = "…" + text[-(STEER_MAX_CHARS - 1) :]
    return STEER_PROMPT.format(text=text)


def render_resume_prompt(message: str) -> str:
    return f"{message.strip()}\n{RECHECK}"
