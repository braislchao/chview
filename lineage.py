import re
from dataclasses import dataclass, field

import pandas as pd
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowEdge, StreamlitFlowNode
from streamlit_flow.layouts import ManualLayout
from streamlit_flow.state import StreamlitFlowState


@dataclass
class TableNode:
    database: str
    name: str
    engine: str
    full_name: str = ""

    def __post_init__(self):
        if not self.full_name:
            self.full_name = f"{self.database}.{self.name}"


@dataclass
class LineageEdge:
    source: str
    target: str
    mv_name: str


@dataclass
class LineageGraph:
    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)
    mv_names: set = field(default_factory=set)
    target_names: set = field(default_factory=set)


def _qualify_table_name(table_ref, default_database):
    table_ref = table_ref.replace("`", "").strip()
    if "." in table_ref:
        return table_ref
    return f"{default_database}.{table_ref}"


def parse_source_tables(create_query, mv_database):
    match = re.search(r"\bAS\s+SELECT\b", create_query, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    select_part = create_query[match.start():]
    table_pattern = r"(?:`[^`]+`|[a-zA-Z_]\w*)(?:\.(?:`[^`]+`|[a-zA-Z_]\w*))?"
    sources = set()

    from_pattern = rf"\bFROM\s+({table_pattern})"
    for m in re.finditer(from_pattern, select_part, re.IGNORECASE):
        sources.add(_qualify_table_name(m.group(1), mv_database))

    join_pattern = rf"\bJOIN\s+({table_pattern})"
    for m in re.finditer(join_pattern, select_part, re.IGNORECASE):
        sources.add(_qualify_table_name(m.group(1), mv_database))

    return sorted(sources)


def parse_target_table(create_query, mv_database, mv_name):
    table_pattern = r"(?:`[^`]+`|[a-zA-Z_]\w*)(?:\.(?:`[^`]+`|[a-zA-Z_]\w*))?"
    to_match = re.search(rf"\bTO\s+({table_pattern})", create_query, re.IGNORECASE)
    if to_match:
        return _qualify_table_name(to_match.group(1), mv_database), False
    return f"{mv_database}.`.inner.{mv_name}`", True


def build_lineage(mv_df, schema_df=None):
    """Build lineage graph from materialized views.

    Args:
        mv_df: DataFrame with materialized view information
        schema_df: Optional DataFrame with table schema info (database, name, engine)
    """
    lineage = LineageGraph()

    # Build lookup for actual engine types from schema
    engine_lookup = {}
    if schema_df is not None and not schema_df.empty:
        for _, row in schema_df.iterrows():
            full_name = f"{row['database']}.{row['name']}"
            engine_lookup[full_name] = row.get('engine', 'Unknown')

    for _, row in mv_df.iterrows():
        db = row["database"]
        mv_name = row["name"]
        create_query = row["create_table_query"]
        mv_full_name = f"{db}.{mv_name}"

        lineage.nodes[mv_full_name] = TableNode(db, mv_name, "MaterializedView")
        lineage.mv_names.add(mv_full_name)

        sources = parse_source_tables(create_query, db)

        deps_db = row.get("dependencies_database", [])
        deps_table = row.get("dependencies_table", [])
        if isinstance(deps_db, (list, tuple)) and isinstance(deps_table, (list, tuple)):
            for dep_db, dep_tbl in zip(deps_db, deps_table):
                dep_full = f"{dep_db}.{dep_tbl}"
                if dep_full not in sources:
                    sources.append(dep_full)

        for source in sources:
            if source not in lineage.nodes:
                parts = source.split(".", 1)
                source_db = parts[0] if len(parts) > 1 else db
                source_name = parts[1] if len(parts) > 1 else parts[0]
                # Get actual engine from schema lookup, fallback to "Source"
                actual_engine = engine_lookup.get(source, "Source")
                lineage.nodes[source] = TableNode(source_db, source_name, actual_engine)
            lineage.edges.append(LineageEdge(source, mv_full_name, mv_full_name))

        target, is_implicit = parse_target_table(create_query, db, mv_name)
        if target not in lineage.nodes:
            parts = target.split(".", 1)
            target_db = parts[0] if len(parts) > 1 else db
            target_name = parts[1] if len(parts) > 1 else parts[0]
            # Get actual engine from schema lookup, fallback to "implicit" or "target"
            actual_engine = engine_lookup.get(target)
            if actual_engine is None:
                actual_engine = "implicit" if is_implicit else "target"
            lineage.nodes[target] = TableNode(target_db, target_name, actual_engine)
        lineage.target_names.add(target)
        lineage.edges.append(LineageEdge(mv_full_name, target, mv_full_name))

    return lineage


def _format_bytes(n):
    if n is None:
        return "-"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def _format_rows(n):
    if n is None or n == 0:
        return "0"
    n = float(n)
    if n < 1_000:
        return f"{n:.0f}"
    for unit in ["K", "M", "B"]:
        if abs(n) < 1_000:
            return f"{n:.1f}{unit}"
        n /= 1_000
    return f"{n:.1f}T"


_NODE_STYLES = {
    "source": {"bg": "#E6F5F6", "border": "#007C85"},
    "MaterializedView": {"bg": "#FDE8EA", "border": "#E51943"},
    "target": {"bg": "#FAF0E8", "border": "#C4836A"},
    "implicit": {"bg": "#F0EDED", "border": "#B0A8A8"},
}


def _resolve_engine(full_name, lineage):
    if full_name in lineage.mv_names:
        return "MaterializedView"
    if full_name in lineage.target_names:
        node = lineage.nodes.get(full_name)
        return "implicit" if node and node.engine == "implicit" else "target"
    return "source"


def _calculate_positions(lineage):
    levels = {}
    visiting = set()

    def get_level(node_id):
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

    nodes_by_level = {}
    for node_id, level in levels.items():
        if level not in nodes_by_level:
            nodes_by_level[level] = []
        nodes_by_level[level].append(node_id)

    incoming_map = {}
    for e in lineage.edges:
        if e.target not in incoming_map:
            incoming_map[e.target] = []
        incoming_map[e.target].append(e.source)

    positions = {}
    x_spacing = 320
    y_spacing = 110

    for level in sorted(nodes_by_level.keys()):
        nodes = nodes_by_level[level]
        x = level * x_spacing + 50

        if level == 0:
            nodes_sorted = sorted(nodes)
        else:
            def sort_key(n):
                parents = incoming_map.get(n, [])
                if parents:
                    parent_y_avg = sum(positions[p][1] for p in parents if p in positions) / len(parents)
                    return parent_y_avg
                return 0
            nodes_sorted = sorted(nodes, key=sort_key)

        total_height = (len(nodes_sorted) - 1) * y_spacing
        start_y = -total_height / 2
        for i, node_id in enumerate(nodes_sorted):
            y = start_y + i * y_spacing
            positions[node_id] = (x, y)

    return positions


def _get_connected_subgraph(lineage, selected_id):
    """Return the set of all node IDs connected to *selected_id* (full upstream + downstream)."""
    connected = {selected_id}

    # Build adjacency lists once
    downstream = {}  # source -> [targets]
    upstream = {}    # target -> [sources]
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


def _build_flow_state(lineage, positions, error_views, connected):
    """Build a StreamlitFlowState with styles based on the connected subgraph."""
    flow_nodes = []
    flow_edges = []

    for full_name, node in lineage.nodes.items():
        engine = _resolve_engine(full_name, lineage)
        style = _NODE_STYLES.get(engine, _NODE_STYLES["source"])
        has_error = full_name in error_views
        is_dimmed = connected is not None and full_name not in connected

        name = node.name if len(node.name) <= 30 else node.name[:27] + "..."

        has_incoming = any(e.target == full_name for e in lineage.edges)
        has_outgoing = any(e.source == full_name for e in lineage.edges)
        if has_incoming and has_outgoing:
            node_type = "default"
        elif has_outgoing:
            node_type = "input"
        else:
            node_type = "output"

        if engine == "MaterializedView":
            border_radius = "50%"
            min_width = "100px"
            padding = "16px 20px"
        else:
            border_radius = "10px"
            min_width = "120px"
            padding = "10px 14px"

        node_style = {
            "background": style["bg"],
            "border": f"2px solid {style['border']}",
            "borderRadius": border_radius,
            "padding": padding,
            "fontSize": "13px",
            "fontFamily": "Inter, -apple-system, sans-serif",
            "fontWeight": "500",
            "color": "#25253D",
            "minWidth": min_width,
            "textAlign": "center",
            "cursor": "pointer",
            "boxShadow": "0 1px 3px rgba(0,0,0,0.1)",
        }

        if has_error and not is_dimmed:
            node_style["border"] = "2px solid #E51943"
            node_style["boxShadow"] = "0 0 10px rgba(229, 25, 67, 0.3)"

        if is_dimmed:
            node_style["opacity"] = "0.12"
            node_style["boxShadow"] = "none"

        pos = positions.get(full_name, (0, 0))

        flow_nodes.append(
            StreamlitFlowNode(
                id=full_name,
                pos=pos,
                data={"content": name},
                node_type=node_type,
                source_position="right",
                target_position="left",
                style=node_style,
                draggable=True,
                selectable=True,
                connectable=False,
                deletable=False,
            )
        )

    for i, edge in enumerate(lineage.edges):
        is_mv = edge.source in lineage.mv_names
        edge_color = "#E51943" if is_mv else "#007C85"
        is_dimmed = connected is not None and (
            edge.source not in connected or edge.target not in connected
        )

        edge_style = {"stroke": edge_color, "strokeWidth": 2}
        if is_dimmed:
            edge_style["opacity"] = "0.06"

        flow_edges.append(
            StreamlitFlowEdge(
                id=f"e{i}",
                source=edge.source,
                target=edge.target,
                edge_type="default",
                animated=not is_dimmed,
                marker_end={"type": "arrowclosed", "color": edge_color},
                style=edge_style,
            )
        )

    return StreamlitFlowState(nodes=flow_nodes, edges=flow_edges)


def render_lineage_graph(lineage, error_views=None):
    import streamlit as st

    error_views = error_views or set()

    # --- Highlight state ---
    highlight_node = st.session_state.get("lineage_highlight")
    connected = None
    if highlight_node and highlight_node in lineage.nodes:
        connected = _get_connected_subgraph(lineage, highlight_node)
    else:
        st.session_state.pop("lineage_highlight", None)
        highlight_node = None

    # --- Cached flow state keyed by highlight to avoid unnecessary re-renders ---
    state_key = f"_lineage_state_{highlight_node}"
    pos_key = "_lineage_positions"
    ever_rendered_key = "_lineage_ever_rendered"

    flow_state = st.session_state.get(state_key)

    if flow_state is None:
        # Pull positions from previous interactions (preserves drag)
        cached_positions = st.session_state.get(pos_key, {})
        computed_positions = _calculate_positions(lineage)
        positions = {}
        for node_id in lineage.nodes:
            positions[node_id] = cached_positions.get(
                node_id, computed_positions.get(node_id, (0, 0))
            )

        flow_state = _build_flow_state(lineage, positions, error_views, connected)
        st.session_state[state_key] = flow_state

    first_render = not st.session_state.get(ever_rendered_key, False)
    st.session_state[ever_rendered_key] = True

    # --- Render (same state object on repeated reruns = no React re-render) ---
    result = streamlit_flow(
        "lineage_flow",
        flow_state,
        layout=ManualLayout(),
        fit_view=first_render,
        height=800,
        show_controls=True,
        show_minimap=False,
        get_node_on_click=True,
        get_edge_on_click=False,
        pan_on_drag=True,
        allow_zoom=True,
        min_zoom=0.3,
        allow_new_edges=False,
        hide_watermark=True,
    )

    # Cache positions from component result (preserves drag across highlight changes)
    if result is not None:
        pos_dict = {}
        for n in result.nodes:
            p = n.position
            pos_dict[n.id] = (p["x"], p["y"])
        st.session_state[pos_key] = pos_dict
        st.session_state[state_key] = result

    # Skip stale result on highlight-induced reruns to prevent toggle loop
    if st.session_state.pop("_lineage_skip_result", False):
        return highlight_node

    # --- Handle click: toggle highlight ---
    if result is not None and result.selected_id:
        if result.selected_id == highlight_node:
            st.session_state.pop("lineage_highlight", None)
        else:
            st.session_state["lineage_highlight"] = result.selected_id
        # Invalidate the old state so the new highlight triggers a rebuild
        st.session_state.pop(state_key, None)
        st.session_state["_lineage_skip_result"] = True
        st.rerun()

    return highlight_node
