from swarm.harness.own.observe import compress_ax_tree, truncate_ax_tree


def ax_node(i: int, role: str, name: str, *, backend: int | None = None, **extra) -> dict:
    return {
        "nodeId": str(i),
        "ignored": False,
        "role": {"type": "role", "value": role},
        "name": {"type": "computedString", "value": name},
        "backendDOMNodeId": backend if backend is not None else 1000 + i,
        **extra,
    }


def load_ax() -> list[dict]:
    """A long page: lots of prose, a few links/buttons/inputs scattered through it."""
    nodes = [ax_node(1, "RootWebArea", "Long page"), ax_node(2, "generic", "")]
    nodes[1]["ignored"] = True
    for i in range(3, 400):
        if i % 40 == 0:
            nodes.append(ax_node(i, "link", f"link number {i}"))
        elif i % 55 == 0:
            nodes.append(ax_node(i, "button", f"button {i}"))
        elif i == 77:
            nodes.append(ax_node(i, "textbox", "search", value={"type": "string", "value": "q"}))
        else:
            nodes.append(ax_node(i, "StaticText", f"paragraph {i} " + "lorem ipsum " * 8))
    return nodes


def test_compress_keeps_interactive_nodes_and_limit():
    text, refs = compress_ax_tree(load_ax(), max_chars=8000)
    assert len(text) <= 8000 and "@e1" in text and refs["@e1"]
    assert 'link "link number 40"' in text
    assert 'textbox "search' in text
    assert all(isinstance(v, str) for v in refs.values())


def test_compress_ranks_interactive_over_prose_when_tight():
    text, refs = compress_ax_tree(load_ax(), max_chars=900)
    assert len(text) <= 900
    assert 'link "link number 40"' in text
    assert "paragraph 100" not in text


def test_compress_spills_long_names_for_recall():
    nodes = [ax_node(1, "RootWebArea", "p"), ax_node(2, "link", "x" * 600)]
    spill: dict[str, str] = {}
    text, refs = compress_ax_tree(nodes, max_chars=8000, spill=spill)
    assert "x" * 600 not in text
    (key,) = spill
    assert spill[key] == "x" * 600 and key in text


def test_refs_map_to_backend_node_ids_and_skip_ignored():
    nodes = [ax_node(1, "button", "ok", backend=42), ax_node(2, "button", "hidden", backend=43)]
    nodes[1]["ignored"] = True
    text, refs = compress_ax_tree(nodes)
    assert refs == {"@e1": "42"} and "hidden" not in text


def test_naive_truncates_without_ranking():
    text, refs = truncate_ax_tree(load_ax(), max_chars=900)
    assert len(text) <= 900
    assert "paragraph 3 " in text  # document order, prose included
    assert 'link "link number 360"' not in text  # cut off, not ranked in
    assert all(f"{r} " in text or text.count(r) for r in refs)
