"""Accessibility tree -> compact text with `@e<n>` refs.

Input is `Accessibility.getFullAXTree` `nodes`. Output lines look like
`@e3 link "Pricing"` for interactive nodes (they get a ref) and `heading "Title"` / `text "..."`
for context. Refs map to the node's backendDOMNodeId (as a string) and are only valid until
the next snapshot.
"""

INTERACTIVE = frozenset(
    {
        "link", "button", "textbox", "searchbox", "combobox", "checkbox", "radio", "menuitem",
        "menuitemcheckbox", "menuitemradio", "tab", "switch", "slider", "spinbutton", "option",
        "listbox", "treeitem", "textarea", "DisclosureTriangle",
    }
)  # fmt: skip
SKIP_ROLES = frozenset({"none", "generic", "presentation", "InlineTextBox", "LineBreak", ""})
NAME_LIMIT = 200  # longer names are shortened in the text; the full text goes to the spill store


def _val(field: object) -> str:
    if isinstance(field, dict):
        v = field.get("value")
        return "" if v is None else str(v)
    return ""


class _Entry:
    __slots__ = ("backend", "interactive", "priority", "role", "text")

    def __init__(self, role: str, text: str, backend: str | None, interactive: bool, priority: int):
        self.role, self.text, self.backend = role, text, backend
        self.interactive, self.priority = interactive, priority


def _entries(nodes: list[dict], *, keep_all: bool) -> list[_Entry]:
    by_id = {n.get("nodeId"): n for n in nodes}
    out: list[_Entry] = []
    for n in nodes:
        if n.get("ignored"):
            continue
        role, name = _val(n.get("role")), _val(n.get("name")).strip()
        value = _val(n.get("value")).strip()
        if role in SKIP_ROLES:
            continue
        interactive = role in INTERACTIVE and n.get("backendDOMNodeId") is not None
        if not interactive and not name:
            continue
        if role == "StaticText" and not keep_all:
            parent = by_id.get(n.get("parentId"))
            if parent is not None and _val(parent.get("role")) in INTERACTIVE:
                continue  # the parent already shows this text
        text = name.replace("\n", " ")
        if value and value != name:
            text += f" = {value}"
        backend = str(n["backendDOMNodeId"]) if interactive else None
        priority = 0 if interactive else (1 if role in ("heading", "RootWebArea") else 2)
        out.append(
            _Entry("text" if role == "StaticText" else role, text, backend, interactive, priority)
        )
    return out


def _render(
    entries: list[_Entry], max_chars: int, spill: dict[str, str] | None
) -> tuple[str, dict[str, str]]:
    lines: list[str] = []
    refs: dict[str, str] = {}
    used = 0
    spilled = 0
    for e in entries:
        text = e.text
        if len(text) > NAME_LIMIT:
            if spill is not None:
                spilled += 1
                key = f"#s{len(spill) + 1}"
                spill[key] = text
                text = f"{text[:NAME_LIMIT]}… [{key} browser_recall로 전체]"
            else:
                text = text[:NAME_LIMIT] + "…"
        n = len(refs) + 1
        prefix = f"@e{n} " if e.interactive else ""
        line = f'{prefix}{e.role} "{text}"'
        if used + len(line) + 1 > max_chars:
            break
        if e.interactive:
            assert e.backend is not None
            refs[f"@e{n}"] = e.backend
        lines.append(line)
        used += len(line) + 1
    return "\n".join(lines), refs


def compress_ax_tree(
    nodes: list[dict], *, max_chars: int = 8000, spill: dict[str, str] | None = None
) -> tuple[str, dict[str, str]]:
    """Ranked compression: if everything fits, keep document order. Otherwise drop prose first,
    then headings, keeping interactive nodes (in document order) until the budget is used.
    `spill`, when given, receives full text of names longer than NAME_LIMIT (key `#s<n>`)."""
    entries = _entries(nodes, keep_all=False)
    for cutoff in (2, 1, 0):  # lowest rank to drop first
        kept = [e for e in entries if e.priority <= cutoff]
        probe_spill = {} if spill is not None else None
        text, _ = _render(kept, max_chars, probe_spill)
        if len(text.splitlines()) == len(kept) or cutoff == 0:
            return _render(kept, max_chars, spill)
    raise AssertionError("unreachable")


def truncate_ax_tree(
    nodes: list[dict], *, max_chars: int = 8000, spill: dict[str, str] | None = None
) -> tuple[str, dict[str, str]]:
    """`naive` policy: same line format, document order, no ranking — just cut at the limit."""
    return _render(_entries(nodes, keep_all=True), max_chars, spill)


# Observation mode (ContextPolicy.observe / swarm-browser-mcp --policy) -> renderer.
OBSERVERS = {"naive": truncate_ax_tree, "compressed": compress_ax_tree}
