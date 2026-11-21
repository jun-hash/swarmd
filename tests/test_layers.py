"""The allowed dependency direction between swarmd's layers, enforced on import statements.

Imports are read with `ast`, so a name used in a string or a comment never counts. Imports under
`if TYPE_CHECKING:` are checked like any other; a failure lists them apart from runtime imports.

Four kinds of rule, all from the layer diagram in the package docstring's architecture map:
- rank: a module imports its own layer or a lower one (`LAYERS`);
- peers: packages of one layer import each other only where `SAME_LAYER_ALLOWED` says;
- never: edges the diagram forbids even though the rank would allow them (`NEVER`);
- narrow: a layer reaches into another package only through the listed modules (`ONLY_VIA`).
"""

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
PACKAGE = "swarm"
SHARED = 0  # rank of the shared layer: its modules import one another freely

# lowest first; a module belongs to the layer of its longest matching prefix
LAYERS: list[tuple[str, list[str]]] = [
    (
        "shared",
        [
            "swarm",
            "swarm.types",
            "swarm.ids",
            "swarm.clock",
            "swarm.ports",
            "swarm.config",
            "swarm.aio",
            "swarm.backoff",
            "swarm.redact",
            "swarm.text",
        ],
    ),
    ("store", ["swarm.store"]),
    ("adapters", ["swarm.engine", "swarm.browser", "swarm.harness"]),
    ("domain", ["swarm.projects", "swarm.inbox", "swarm.scheduler"]),
    ("orchestration", ["swarm.coordinator", "swarm.workers", "swarm.brain"]),
    ("front", ["swarm.front"]),
    ("process", ["swarm.daemon", "swarm.watchdog", "swarm.cli", "swarm.housekeeping"]),
]

# Same-layer imports between different packages. Everything else across packages of one layer
# is forbidden: adapters never import each other's internals, domain packages share only the
# Protocols in ports.py, the coordinator and the pool never import each other.
SAME_LAYER_ALLOWED: dict[str, list[str]] = {
    "swarm.harness": ["swarm.engine.base", "swarm.browser.cdp"],
    "swarm.coordinator": ["swarm.workers.contract", "swarm.brain"],
    "swarm.workers": [],
    "swarm.brain": [],
    "swarm.daemon": ["swarm.watchdog", "swarm.cli", "swarm.housekeeping"],
    "swarm.watchdog": ["swarm.daemon", "swarm.cli", "swarm.housekeeping"],
    "swarm.cli": ["swarm.daemon", "swarm.watchdog", "swarm.housekeeping"],
    "swarm.housekeeping": [],
}

# Lower-layer edges the diagram still forbids.
NEVER: dict[str, list[str]] = {
    "swarm.store": ["swarm.daemon"],
    "swarm.engine": ["swarm.browser", "swarm.harness"],
    "swarm.browser": ["swarm.engine", "swarm.harness"],
    "swarm.harness": ["swarm.engine.claude", "swarm.browser.guard", "swarm.browser.chrome"],
    "swarm.projects": ["swarm.engine", "swarm.browser", "swarm.harness"],
    "swarm.inbox": ["swarm.engine", "swarm.browser", "swarm.harness"],
    "swarm.scheduler": ["swarm.engine", "swarm.browser", "swarm.harness"],
    "swarm.workers": ["swarm.browser", "swarm.engine.claude", "swarm.harness.own"],
    "swarm.coordinator": ["swarm.browser", "swarm.engine", "swarm.harness"],
    "swarm.front": ["swarm.workers", "swarm.browser", "swarm.engine", "swarm.harness"],
}

# A layer may use these packages only through the listed modules: workers see the harness and
# engine seams, never a concrete adapter or a registry.
ONLY_VIA: dict[str, dict[str, list[str]]] = {
    "swarm.workers": {
        "swarm.harness": ["swarm.harness.base"],
        "swarm.engine": ["swarm.engine.base"],
    },
}

# Target edges that are tolerated for now: (importer, imported) -> why the edge is tolerated.
# An entry whose import is gone fails `test_known_violations_still_exist`.
KNOWN_VIOLATIONS: dict[tuple[str, str], str] = {}


@dataclass(frozen=True)
class Edge:
    importer: str
    imported: str
    line: int
    type_checking: bool

    def __str__(self) -> str:
        where = " (TYPE_CHECKING)" if self.type_checking else ""
        return f"{self.importer} -> {self.imported}  [line {self.line}]{where}"


def _under(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


def _module_name(path: Path) -> str:
    parts = path.relative_to(SRC).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _is_module(name: str) -> bool:
    base = SRC.joinpath(*name.split("."))
    return base.with_suffix(".py").exists() or (base / "__init__.py").exists()


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _imports(node: ast.AST, guarded: bool = False) -> Iterator[tuple[ast.stmt, bool]]:
    """Every import statement under `node`, wherever it sits (functions, try/except, match),
    with whether it is inside `if TYPE_CHECKING:`."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.Import | ast.ImportFrom):
            yield child, guarded
        elif isinstance(child, ast.If) and _is_type_checking(child.test):
            for stmt in child.body:
                yield from _imports(ast.Module(body=[stmt], type_ignores=[]), True)
            for stmt in child.orelse:
                yield from _imports(ast.Module(body=[stmt], type_ignores=[]), guarded)
        else:
            yield from _imports(child, guarded)


def _targets(node: ast.stmt) -> list[tuple[str, list[str]]]:
    """(module, imported names) of an import; empty for anything outside the package."""
    if isinstance(node, ast.Import):
        return [(a.name, []) for a in node.names if a.name.split(".")[0] == PACKAGE]
    assert isinstance(node, ast.ImportFrom)
    assert node.level == 0, "relative imports hide the layer an edge crosses"
    if node.module is None or node.module.split(".")[0] != PACKAGE:
        return []
    return [(node.module, [a.name for a in node.names])]


def source_files() -> list[Path]:
    return sorted(SRC.joinpath(PACKAGE).rglob("*.py"))


def collect_edges() -> list[Edge]:
    edges: list[Edge] = []
    for path in source_files():
        importer = _module_name(path)
        for node, guarded in _imports(ast.parse(path.read_text(), filename=str(path))):
            for module, names in _targets(node):
                imported = {module}
                imported |= {f"{module}.{n}" for n in names if _is_module(f"{module}.{n}")}
                edges += [Edge(importer, i, node.lineno, guarded) for i in sorted(imported)]
    return edges


def layer_of(module: str) -> int | None:
    best: tuple[int, int] | None = None  # (prefix length, rank)
    for rank, (_, prefixes) in enumerate(LAYERS):
        for prefix in prefixes:
            if _under(module, prefix) and (best is None or len(prefix) > best[0]):
                best = (len(prefix), rank)
    return None if best is None else best[1]


def _package(module: str) -> str:
    """The top-level package of a module: swarm.workers.pool -> swarm.workers."""
    parts = module.split(".")
    return ".".join(parts[:2])


def breaks_a_rule(edge: Edge) -> bool:
    src, dst = layer_of(edge.importer), layer_of(edge.imported)
    assert src is not None and dst is not None, edge
    if dst > src:
        return True  # rank: never upward
    if _package(edge.importer) != _package(edge.imported):
        if (
            dst == src
            and src != SHARED
            and not any(
                _under(edge.imported, ok)
                for ok in SAME_LAYER_ALLOWED.get(_package(edge.importer), [])
            )
        ):
            return True
        if any(_under(edge.imported, b) for b in NEVER.get(_package(edge.importer), [])):
            return True
        for package, via in ONLY_VIA.get(_package(edge.importer), {}).items():
            if _under(edge.imported, package) and not any(_under(edge.imported, v) for v in via):
                return True
    return False


def violations() -> list[Edge]:
    return [e for e in collect_edges() if breaks_a_rule(e)]


def test_every_module_is_in_a_layer() -> None:
    unmapped = sorted(
        {m for e in collect_edges() for m in (e.importer, e.imported) if layer_of(m) is None}
    )
    assert not unmapped, f"add these modules to LAYERS: {unmapped}"


def test_no_forbidden_imports() -> None:
    new = [e for e in violations() if (e.importer, e.imported) not in KNOWN_VIOLATIONS]
    runtime = [f"  {e}" for e in new if not e.type_checking]
    typing_only = [f"  {e}" for e in new if e.type_checking]
    assert not new, (
        "forbidden imports:\n"
        + "\n".join(runtime)
        + (
            "\nforbidden imports under TYPE_CHECKING:\n" + "\n".join(typing_only)
            if typing_only
            else ""
        )
    )


def test_known_violations_still_exist() -> None:
    present = {(e.importer, e.imported) for e in violations()}
    stale = sorted(set(KNOWN_VIOLATIONS) - present)
    assert not stale, f"remove from KNOWN_VIOLATIONS, the import is gone: {stale}"


def test_no_private_names_cross_modules() -> None:
    bad = []
    for path in source_files():
        for node, _ in _imports(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                if node.module.split(".")[0] == PACKAGE:
                    bad += [
                        f"{_module_name(path)}: from {node.module} import {a.name}"
                        for a in node.names
                        if a.name.startswith("_")
                    ]
    assert not bad, "private names imported across modules:\n" + "\n".join(f"  {b}" for b in bad)


@pytest.mark.parametrize(
    ("importer", "imported", "forbidden"),
    [
        ("swarm.ports", "swarm.workers.contract", True),
        ("swarm.config", "swarm.types", False),
        ("swarm.inbox.inbox", "swarm.projects.store", True),
        ("swarm.coordinator.coordinator", "swarm.workers.pool", True),
        ("swarm.coordinator.coordinator", "swarm.workers.contract", False),
        ("swarm.workers.pool", "swarm.engine.registry", True),
        ("swarm.workers.pool", "swarm.engine.base", False),
        ("swarm.harness.registry", "swarm.engine.simulate", True),
        ("swarm.harness.own.adapter", "swarm.browser.cdp", False),
        ("swarm.harness.own.adapter", "swarm.browser.requests", True),
        ("swarm.engine.claude", "swarm.browser.cdp", True),
        ("swarm.front.mcp_server", "swarm.workers.contract", True),
        ("swarm.brain", "swarm.coordinator.agenda", True),
        ("swarm.watchdog", "swarm.workers.contract", False),
    ],
)
def test_the_rules_bite(importer: str, imported: str, forbidden: bool) -> None:
    """The rule tables above are not vacuous: each case is a diagram edge."""
    assert breaks_a_rule(Edge(importer, imported, 1, False)) is forbidden
