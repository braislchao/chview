"""Node positioning and layout algorithms for the lineage graph."""

from chview.lineage.graph import LineageGraph


def _find_clusters(lineage: LineageGraph) -> list[set[str]]:
    """Find connected components (clusters) in the graph.

    Each cluster is a set of node IDs that are transitively connected.
    """
    downstream: dict[str, list[str]] = {}
    upstream: dict[str, list[str]] = {}
    for edge in lineage.edges:
        downstream.setdefault(edge.source, []).append(edge.target)
        upstream.setdefault(edge.target, []).append(edge.source)

    visited: set[str] = set()
    clusters: list[set[str]] = []

    for node_id in lineage.nodes:
        if node_id in visited:
            continue
        cluster: set[str] = set()
        queue = [node_id]
        while queue:
            n = queue.pop(0)
            if n in cluster:
                continue
            cluster.add(n)
            for child in downstream.get(n, []):
                if child not in cluster:
                    queue.append(child)
            for parent in upstream.get(n, []):
                if parent not in cluster:
                    queue.append(parent)
        visited |= cluster
        clusters.append(cluster)

    return clusters


def _get_cluster_sort_key(cluster: set[str]) -> str:
    """Sort key for a cluster: alphabetically by earliest source node name."""
    return min(sorted(cluster))


def calculate_positions(lineage: LineageGraph) -> dict[str, tuple[float, float]]:
    """Calculate (x, y) positions for all nodes using cluster-aware layout.

    Groups nodes into connected clusters (pipelines), lays out each cluster
    as a vertical band sorted by source name, and separates clusters with
    gaps for visual clarity.

    Args:
        lineage: The lineage graph to lay out

    Returns:
        Dict mapping node_id -> (x, y) position
    """
    if not lineage.nodes:
        return {}

    # --- Compute topological levels ---
    levels: dict[str, int] = {}
    visiting: set[str] = set()

    def get_level(node_id: str) -> int:
        if node_id in levels:
            return levels[node_id]
        if node_id in visiting:
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

    # --- Build adjacency maps ---
    incoming_map: dict[str, list[str]] = {}
    for e in lineage.edges:
        incoming_map.setdefault(e.target, []).append(e.source)

    # --- Find and sort clusters ---
    clusters = _find_clusters(lineage)
    clusters.sort(key=_get_cluster_sort_key)

    # --- Layout constants ---
    x_spacing = 380
    y_spacing = 130
    cluster_gap = 80  # extra vertical gap between clusters

    positions: dict[str, tuple[float, float]] = {}
    current_y = 0.0

    for cluster in clusters:
        # Group cluster nodes by level
        cluster_by_level: dict[int, list[str]] = {}
        for node_id in cluster:
            lvl = levels[node_id]
            cluster_by_level.setdefault(lvl, []).append(node_id)

        # Sort within each level: level 0 alphabetically, others by parent y
        for lvl in sorted(cluster_by_level.keys()):
            nodes = cluster_by_level[lvl]
            if lvl == 0:
                nodes.sort()
            else:

                def sort_key(n: str, _map=incoming_map, _pos=positions) -> float:
                    parents = _map.get(n, [])
                    positioned = [_pos[p][1] for p in parents if p in _pos]
                    return sum(positioned) / len(positioned) if positioned else 0.0

                nodes.sort(key=sort_key)
            cluster_by_level[lvl] = nodes

        # Find the tallest column in this cluster
        max_rows = max(len(ns) for ns in cluster_by_level.values())

        # Place nodes: x by level, y within cluster band
        for lvl in sorted(cluster_by_level.keys()):
            nodes = cluster_by_level[lvl]
            x = float(lvl * x_spacing + 80)
            # Center this column vertically within the cluster band
            col_height = (len(nodes) - 1) * y_spacing
            band_height = (max_rows - 1) * y_spacing
            y_offset = current_y + (band_height - col_height) / 2
            for i, node_id in enumerate(nodes):
                y = y_offset + i * y_spacing
                positions[node_id] = (x, y)

        # Advance y cursor past this cluster
        band_height = (max_rows - 1) * y_spacing
        current_y += band_height + cluster_gap + y_spacing

    # Center everything around y=0
    if positions:
        all_ys = [p[1] for p in positions.values()]
        y_center = (min(all_ys) + max(all_ys)) / 2
        positions = {nid: (x, y - y_center) for nid, (x, y) in positions.items()}

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
