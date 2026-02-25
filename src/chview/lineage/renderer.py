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
    "source": {
        "bg": "#E8F8F7",
        "border": "#07A4AE",
        "badge_bg": "#D0F0ED",
        "badge_text": "#058489",
        "shadow": "rgba(7,164,174,0.12)",
        "label": "Source",
        "icon": "\u22b3",
    },
    "MaterializedView": {
        "bg": "#FCE9ED",
        "border": "#E51745",
        "badge_bg": "#F9D3DB",
        "badge_text": "#C21339",
        "shadow": "rgba(229,23,69,0.10)",
        "label": "Mat. View",
        "icon": "\u2b21",
    },
    "target": {
        "bg": "#F8F2EC",
        "border": "#BF8659",
        "badge_bg": "#F0E2D4",
        "badge_text": "#A06F46",
        "shadow": "rgba(191,134,89,0.12)",
        "label": "Target",
        "icon": "\u22b2",
    },
    "implicit": {
        "bg": "#F1F2F5",
        "border": "#A4ABBA",
        "badge_bg": "#E5E7ED",
        "badge_text": "#636B7F",
        "shadow": "rgba(164,171,186,0.10)",
        "label": "Implicit",
        "icon": "\u2218",
    },
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
    highlight_node: Optional[str] = None,
) -> StreamlitFlowState:
    """Build a StreamlitFlowState with styles based on the connected subgraph."""
    flow_nodes = []
    flow_edges = []

    for full_name, node in lineage.nodes.items():
        engine = _resolve_engine(full_name, lineage)
        style = _NODE_STYLES.get(engine, _NODE_STYLES["source"])
        has_error = full_name in error_views
        is_dimmed = connected is not None and full_name not in connected
        is_highlighted = full_name == highlight_node

        name = node.name if len(node.name) <= 35 else node.name[:32] + "..."
        content = f"**{style['icon']} {style['label']}**\n\n{name}"

        has_incoming = any(e.target == full_name for e in lineage.edges)
        has_outgoing = any(e.source == full_name for e in lineage.edges)
        if has_incoming and has_outgoing:
            node_type = "default"
        elif has_outgoing:
            node_type = "input"
        else:
            node_type = "output"

        border_color = style["border"]
        shadow_color = style["shadow"]

        node_style: dict[str, str] = {
            "background": style["bg"],
            "border": f"2px solid {border_color}",
            "borderRadius": "12px",
            "padding": "16px 20px",
            "fontSize": "14px",
            "fontFamily": "Inter, -apple-system, sans-serif",
            "fontWeight": "400",
            "color": "#0D1525",
            "width": "260px",
            "textAlign": "left",
            "cursor": "pointer",
            "boxShadow": f"0 2px 8px {shadow_color}",
        }

        if is_highlighted and not is_dimmed:
            node_style["boxShadow"] = (
                f"0 0 0 3px {border_color}40, 0 4px 12px {shadow_color}"
            )

        if has_error and not is_dimmed:
            node_style["border"] = "2px solid #FF5C4D"
            node_style["boxShadow"] = (
                "0 0 0 3px rgba(255,92,77,0.15), 0 2px 8px rgba(255,92,77,0.20)"
            )

        if is_dimmed:
            node_style["opacity"] = "0.15"
            node_style["filter"] = "grayscale(0.5)"
            node_style["boxShadow"] = "none"

        pos = positions.get(full_name, (0, 0))

        flow_nodes.append(
            StreamlitFlowNode(
                id=full_name,
                pos=pos,
                data={"content": content},
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
        edge_color = "#E51745" if is_mv else "#07A4AE"
        is_dimmed = connected is not None and (
            edge.source not in connected or edge.target not in connected
        )

        edge_style: dict[str, str] = {"stroke": edge_color, "strokeWidth": "1.5"}
        if is_dimmed:
            edge_style["opacity"] = "0.08"
            edge_style["strokeWidth"] = "1"

        flow_edges.append(
            StreamlitFlowEdge(
                id=f"e{i}",
                source=edge.source,
                target=edge.target,
                edge_type="smoothstep",
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
            node_id: cached_positions.get(
                node_id, computed_positions.get(node_id, (0.0, 0.0))
            )
            for node_id in lineage.nodes
        }

        flow_state = _build_flow_state(
            lineage, positions, error_views, connected, highlight_node
        )
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
        style={"background": "#FAFAFA"},
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
