"""Short text for logs, events and cards."""


def clip(text: str, limit: int) -> str:
    """`text` cut to at most `limit` characters, ending in "…" when cut."""
    return text if len(text) <= limit else text[: limit - 1] + "…"


def error_text(e: BaseException, limit: int) -> str:
    """ "ValueError: boom", cut to `limit` characters."""
    return f"{type(e).__name__}: {e}"[:limit]


def first_line(text: str) -> str:
    """The first line of `text`, stripped ("" for blank text)."""
    lines = text.strip().splitlines()
    return lines[0].strip() if lines else ""
