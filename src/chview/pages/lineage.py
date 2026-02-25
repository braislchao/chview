"""Lineage page: interactive MV data flow visualization."""

from typing import Optional

import pandas as pd
import streamlit as st

from chview.components.tables import render_node_detail_sidebar
from chview.lineage.graph import build_lineage
from chview.lineage.renderer import _NODE_STYLES, _resolve_engine, render_lineage_graph


def render_lineage_page(
    load_materialized_views,
    load_schema,
    load_storage_metrics,
    load_recent_throughput,
    load_mv_errors,
    load_kafka_consumers,
    load_create_table,
    load_create_view,
    database: Optional[str] = None,
) -> None:
    """Render the lineage graph page with click-to-query detail panel.

    Args:
        load_materialized_views: Cached MV loader
        load_schema: Cached schema loader
        load_storage_metrics: Cached storage metrics loader
        load_recent_throughput: Cached recent throughput loader
        load_mv_errors: Cached MV errors loader
        load_kafka_consumers: Cached Kafka consumers loader
        load_create_table: Cached CREATE TABLE fetcher
        load_create_view: Cached CREATE VIEW fetcher
        database: Currently selected database filter
    """
    st.header("Lineage")

    if database and database != "All":
        st.caption(f"Database: **{database}**")

    try:
        with st.spinner("Loading materialized views..."):
            mv_df = load_materialized_views(database=database)
            schema_df = load_schema(database=database)

        if mv_df is None or mv_df.empty:
            st.info("No materialized views found in this cluster.")
            return

        lineage = build_lineage(mv_df, schema_df)

        # Metric cards
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Mat. Views", len(lineage.mv_names))
        with c2:
            st.metric("Tables", len(lineage.nodes))
        with c3:
            st.metric("Edges", len(lineage.edges))
        with c4:
            st.metric("Targets", len(lineage.target_names))

        # Legend
        legend_items = ""
        for style in _NODE_STYLES.values():
            legend_items += (
                '<div style="display: flex; align-items: center; gap: 8px;">'
                f'<span style="width: 28px; height: 16px; background: {style["bg"]}; border: 2px solid {style["border"]}; border-radius: 4px; display: inline-block;"></span>'
                f'<span style="font-size: 13px; color: #636B7F; font-weight: 500;">{style["label"]}</span>'
                "</div>"
            )
        st.markdown(
            f'<div style="display: flex; justify-content: center; align-items: center; gap: 24px; margin-bottom: 1rem; padding: 12px 20px; background: hsl(220 88% 17% / 0.04); border-radius: 8px; border: 1px solid hsl(213 87% 15% / 0.20);">'
            f"{legend_items}"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Load supporting data
        try:
            storage_df = load_storage_metrics(database=database)
        except Exception:
            storage_df = pd.DataFrame()

        # Detect error MVs
        error_views: set[str] = set()
        try:
            error_df = load_mv_errors(database=database)
            if error_df is not None and not error_df.empty:
                error_views = set(error_df["view_name"].unique())
        except Exception:
            pass

        # Toolbar row
        selected_id = st.session_state.get("lineage_highlight")
        toolbar_left, toolbar_right = st.columns([3, 1])
        with toolbar_left:
            if selected_id and selected_id in lineage.nodes:
                node = lineage.nodes[selected_id]
                engine = _resolve_engine(selected_id, lineage)
                style = _NODE_STYLES.get(engine, _NODE_STYLES["source"])
                badge_html = (
                    f'<span style="display: inline-block; padding: 2px 10px; border-radius: 6px;'
                    f' background: {style["badge_bg"]}; color: {style["badge_text"]};'
                    f' font-size: 0.78rem; font-weight: 600; margin-right: 8px;">'
                    f'{style["label"]}</span>'
                )
                st.markdown(
                    f'<div style="font-size: 0.95rem; font-weight: 600; padding: 0.4rem 0;">'
                    f"{badge_html}"
                    f'{node.database}.<span style="color: {style["border"]};">{node.name}</span>'
                    f"</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="font-size: 0.95rem; padding: 0.4rem 0; color: #A4ABBA;">Click a node to highlight its connections</div>',
                    unsafe_allow_html=True,
                )
        with toolbar_right:
            clear_disabled = not bool(selected_id and selected_id in lineage.nodes)
            if st.button(
                "Clear highlight",
                key="clear_highlight",
                type="secondary",
                use_container_width=True,
                disabled=clear_disabled,
            ):
                st.session_state.pop("lineage_highlight", None)
                st.session_state["_lineage_skip_result"] = True
                st.rerun()

        # Two-column layout: graph + side panel
        graph_col, panel_col = st.columns([3, 1])

        with graph_col:
            selected_id = render_lineage_graph(lineage, error_views=error_views)

        with panel_col:
            if selected_id and selected_id in lineage.nodes:
                node = lineage.nodes[selected_id]

                ddl_type = (
                    "MATERIALIZED VIEW" if selected_id in lineage.mv_names else "TABLE"
                )

                panel_engine = _resolve_engine(selected_id, lineage)
                panel_style = _NODE_STYLES.get(panel_engine, _NODE_STYLES["source"])
                st.markdown(
                    f'<div style="font-size: 0.95rem; font-weight: 600; padding: 0.3rem 0 0.5rem;">'
                    f'<span style="display: inline-block; padding: 2px 10px; border-radius: 6px;'
                    f' background: {panel_style["badge_bg"]}; color: {panel_style["badge_text"]};'
                    f' font-size: 0.78rem; font-weight: 600; margin-right: 6px;">'
                    f'{panel_style["label"]}</span>'
                    f"{node.name}</div>",
                    unsafe_allow_html=True,
                )
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Database", node.database)
                with col2:
                    st.metric("Engine", node.engine)

                st.markdown("---")

                try:
                    if selected_id in lineage.mv_names:
                        create_sql = load_create_view(node.database, node.name)
                    else:
                        create_sql = load_create_table(node.database, node.name)
                except Exception as e:
                    if selected_id in lineage.mv_names:
                        create_sql = f"-- Unable to fetch CREATE MATERIALIZED VIEW: {e}"
                    else:
                        create_sql = f"-- Unable to fetch CREATE TABLE: {e}"

                render_node_detail_sidebar(
                    node.database, node.name, storage_df, create_sql, ddl_type
                )
            else:
                st.info("Click on a node to view details")

    except Exception as e:
        st.error(f"Failed to load lineage: {e}")
