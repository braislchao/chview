"""Node positioning and layout algorithms for the lineage graph."""

from chview.lineage.graph import LineageGraph


def calculate_positions(lineage: LineageGraph) -> dict[str, tuple[float, float]]:
    """Calculate (x, y) positions for all nodes using topological level layout.

    Assigns nodes to columns by their topological level (depth from sources),
    then sorts within each column to minimize edge crossings.

    Args:
        lineage: The lineage graph to lay out

    Returns:
        Dict mapping node_id -> (x, y) position
    """
    levels: dict[str, int] = {}
    visiting: set[str] = set()

    def get_level(node_id: str) -> int:
        if node_id in levels:
            return levels[node_id]
        if node_id in visiting:
            # Cycle detected — break it by treating this node as a root
            levels[node_id] = 0
            return 0
        visiting.add(node_id)
        incoming = [e.source for e in lineage.edges if e.target == node_id]
        if not incoming:
            levels[node_id] = 0
        else:
            max_parent_level = max(get_level(parent) for parent in incoming)
            levels[node_id] = max_parent_level + 1
        visiting.discard(node_id)
        return levels[node_id]

    for node_id in lineage.nodes:
        get_level(node_id)

    nodes_by_level: dict[int, list[str]] = {}
    for node_id, level in levels.items():
        nodes_by_level.setdefault(level, []).append(node_id)

    incoming_map: dict[str, list[str]] = {}
    for e in lineage.edges:
        incoming_map.setdefault(e.target, []).append(e.source)

    positions: dict[str, tuple[float, float]] = {}
    x_spacing = 320
    y_spacing = 110

    for level in sorted(nodes_by_level.keys()):
        nodes = nodes_by_level[level]
        x = float(level * x_spacing + 50)

        if level == 0:
            nodes_sorted = sorted(nodes)
        else:

            def sort_key(n: str) -> float:
                parents = incoming_map.get(n, [])
                if parents:
                    return sum(
                        positions[p][1] for p in parents if p in positions
                    ) / len(parents)
                return 0.0

            nodes_sorted = sorted(nodes, key=sort_key)

        total_height = (len(nodes_sorted) - 1) * y_spacing
        start_y = -total_height / 2
        for i, node_id in enumerate(nodes_sorted):
            y = start_y + i * y_spacing
            positions[node_id] = (x, y)

    return positions


def get_connected_subgraph(lineage: LineageGraph, selected_id: str) -> set[str]:
    """Return the set of all node IDs connected to *selected_id*.

    Traverses both upstream (sources) and downstream (targets) using BFS.

    Args:
        lineage: The full lineage graph
        selected_id: Node ID to find connections for

    Returns:
        Set of all directly or transitively connected node IDs (including selected_id)
    """
    connected = {selected_id}

    downstream: dict[str, list[str]] = {}
    upstream: dict[str, list[str]] = {}
    for edge in lineage.edges:
        downstream.setdefault(edge.source, []).append(edge.target)
        upstream.setdefault(edge.target, []).append(edge.source)

    # BFS downstream
    queue = [selected_id]
    while queue:
        node = queue.pop(0)
        for child in downstream.get(node, []):
            if child not in connected:
                connected.add(child)
                queue.append(child)

    # BFS upstream
    queue = [selected_id]
    while queue:
        node = queue.pop(0)
        for parent in upstream.get(node, []):
            if parent not in connected:
                connected.add(parent)
                queue.append(parent)

    return connected
