"""Streamlit Flow rendering for the lineage graph."""

from typing import Optional

import streamlit as st
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowEdge, StreamlitFlowNode
from streamlit_flow.layouts import ManualLayout
from streamlit_flow.state import StreamlitFlowState

from chview.lineage.graph import LineageGraph
from chview.lineage.layout import calculate_positions, get_connected_subgraph

# Node visual styles indexed by engine type
_NODE_STYLES: dict[str, dict[str, str]] = {
    "source": {"bg": "#E6F5F6", "border": "#007C85"},
    "MaterializedView": {"bg": "#FDE8EA", "border": "#E51943"},
    "target": {"bg": "#FAF0E8", "border": "#C4836A"},
    "implicit": {"bg": "#F0EDED", "border": "#B0A8A8"},
}


def _resolve_engine(full_name: str, lineage: LineageGraph) -> str:
    """Determine the visual engine category for a node."""
    if full_name in lineage.mv_names:
        return "MaterializedView"
    if full_name in lineage.target_names:
        node = lineage.nodes.get(full_name)
        return "implicit" if node and node.engine == "implicit" else "target"
    return "source"


def _build_flow_state(
    lineage: LineageGraph,
    positions: dict[str, tuple[float, float]],
    error_views: set[str],
    connected: Optional[set[str]],
) -> StreamlitFlowState:
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

        node_style: dict[str, str] = {
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

        edge_style: dict[str, str] = {"stroke": edge_color, "strokeWidth": "2"}
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


def render_lineage_graph(
    lineage: LineageGraph, error_views: Optional[set[str]] = None
) -> Optional[str]:
    """Render the interactive lineage graph using streamlit-flow.

    Handles highlight state, position caching, and click events.

    Args:
        lineage: The lineage graph to render
        error_views: Set of MV full names that have recent errors

    Returns:
        Currently highlighted node ID, or None
    """
    error_views = error_views or set()

    # --- Highlight state ---
    highlight_node: Optional[str] = st.session_state.get("lineage_highlight")
    connected: Optional[set[str]] = None
    if highlight_node and highlight_node in lineage.nodes:
        connected = get_connected_subgraph(lineage, highlight_node)
    else:
        st.session_state.pop("lineage_highlight", None)
        highlight_node = None

    # --- Cached flow state keyed by highlight to avoid unnecessary re-renders ---
    state_key = f"_lineage_state_{highlight_node}"
    pos_key = "_lineage_positions"
    ever_rendered_key = "_lineage_ever_rendered"

    flow_state: Optional[StreamlitFlowState] = st.session_state.get(state_key)

    if flow_state is None:
        cached_positions: dict[str, tuple[float, float]] = st.session_state.get(
            pos_key, {}
        )
        computed_positions = calculate_positions(lineage)
        positions: dict[str, tuple[float, float]] = {
            node_id: cached_positions.get(node_id, computed_positions.get(node_id, (0.0, 0.0)))
            for node_id in lineage.nodes
        }

        flow_state = _build_flow_state(lineage, positions, error_views, connected)
        st.session_state[state_key] = flow_state

    first_render = not st.session_state.get(ever_rendered_key, False)
    st.session_state[ever_rendered_key] = True

    # --- Render ---
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
        pos_dict: dict[str, tuple[float, float]] = {}
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
        st.session_state.pop(state_key, None)
        st.session_state["_lineage_skip_result"] = True
        st.rerun()

    return highlight_node
