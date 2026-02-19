import streamlit as st

st.set_page_config(page_title="CHView", page_icon="\U0001f4ca", layout="wide")

import pandas as pd

from db import (
    fetch_cluster_info,
    fetch_create_table,
    fetch_create_view,
    fetch_databases,
    fetch_kafka_consumers,
    fetch_materialized_views,
    fetch_mv_errors,
    fetch_mv_throughput,
    fetch_partition_storage,
    fetch_recent_throughput,
    fetch_schema,
    fetch_storage_metrics,
)  # all used via load_* wrappers below
from lineage import build_lineage, render_lineage_graph
from ui_components import (
    format_bytes,
    format_number,
    inject_custom_css,
    render_connection_status,
    render_kafka_consumers,
    render_metrics_cards,
    render_mv_errors_table,
    render_mv_health_banner,
    render_node_detail,
    render_node_detail_sidebar,
    render_schema_sidebar,
    render_sidebar_nav,
    render_storage_treemap,
    render_table_detail,
    render_throughput_charts,
)

inject_custom_css()

# --- Init session state ---

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "overview"

if "selected_database" not in st.session_state:
    st.session_state["selected_database"] = "All"


# --- Cached data loaders ---


@st.cache_data(ttl=120)
def load_schema(database=None):
    return fetch_schema(database)


@st.cache_data(ttl=300)
def load_storage_metrics(database=None):
    return fetch_storage_metrics(database)


@st.cache_data(ttl=120)
def load_materialized_views(database=None):
    return fetch_materialized_views(database)


@st.cache_data(ttl=300)
def load_throughput(hours=24, database=None):
    return fetch_mv_throughput(hours, database)


@st.cache_data(ttl=120)
def load_cluster_info(database=None):
    return fetch_cluster_info(database)


@st.cache_data(ttl=300)
def load_partition_storage(database=None):
    return fetch_partition_storage(database)


@st.cache_data(ttl=300)
def load_recent_throughput(minutes=30, database=None):
    return fetch_recent_throughput(minutes, database)


@st.cache_data(ttl=60)
def load_mv_errors(hours=24, database=None):
    return fetch_mv_errors(hours, database)


@st.cache_data(ttl=60)
def load_kafka_consumers(database=None):
    return fetch_kafka_consumers(database)


@st.cache_data(ttl=120)
def load_create_table(database, table):
    return fetch_create_table(database, table)


@st.cache_data(ttl=120)
def load_create_view(database, view):
    return fetch_create_view(database, view)


@st.cache_data(ttl=300)
def load_databases():
    return fetch_databases()


# ---------------------------------------------------------------------------
# Page functions
# ---------------------------------------------------------------------------


def render_overview_page(database=None):
    """Overview dashboard with cluster metrics and table listing."""
    st.header("Overview")

    # Show selected database
    if database and database != "All":
        st.caption(f"Database: **{database}**")

    # MV health banner
    try:
        error_df = load_mv_errors(database=database)
        render_mv_health_banner(error_df)
    except Exception:
        pass

    try:
        info = load_cluster_info(database=database)
    except Exception as e:
        st.error(f"Failed to load cluster info: {e}")
        return

    # Metric cards row
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Version", info["version"])
    with c2:
        uptime_h = info["uptime_seconds"] / 3600
        if uptime_h >= 24:
            st.metric("Uptime", f"{uptime_h / 24:.1f} days")
        else:
            st.metric("Uptime", f"{uptime_h:.1f} hrs")
    with c3:
        st.metric("Databases", info["user_databases"])
    with c4:
        st.metric("Tables", info["user_tables"])
    with c5:
        st.metric("Mat. Views", info["mv_count"])

    st.divider()

    # Two-column layout: table listing + engine breakdown
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown('<div class="lenses-card-title">Table Overview</div>', unsafe_allow_html=True)
        try:
            schema_df = load_schema(database=database)
            if schema_df is not None and not schema_df.empty:
                display_df = schema_df[["database", "name", "engine", "total_rows", "total_bytes"]].copy()
                display_df["total_rows"] = display_df["total_rows"].apply(format_number)
                display_df["total_bytes"] = display_df["total_bytes"].apply(format_bytes)
                display_df.columns = ["Database", "Name", "Engine", "Rows", "Size"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No tables found.")
        except Exception as e:
            st.error(f"Failed to load schema: {e}")

    with col_right:
        st.markdown('<div class="lenses-card-title">Engine Breakdown</div>', unsafe_allow_html=True)
        try:
            import plotly.express as px

            schema_df = load_schema(database=database)
            if schema_df is not None and not schema_df.empty:
                engine_counts = schema_df["engine"].value_counts().reset_index()
                engine_counts.columns = ["Engine", "Count"]
                fig = px.pie(
                    engine_counts,
                    values="Count",
                    names="Engine",
                    color_discrete_sequence=[
                        "#E8A5AF", "#8FD3D8", "#E8C4B0",
                        "#F5D4A1", "#F0B8C0", "#A8B5C4",
                        "#C8C8C8", "#D8C8C8",
                    ],
                    hole=0.4,
                )
                fig.update_layout(
                    margin=dict(t=10, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(font=dict(size=11)),
                    height=300,
                )
                fig.update_traces(textinfo="label+percent", textfont_size=11)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data for chart.")
        except Exception as e:
            st.error(f"Failed to load engine breakdown: {e}")


def render_lineage_page(database=None):
    """Lineage graph page with click-to-query detail panel."""
    st.header("Lineage")

    # Show selected database
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

        # Legend with soft pastel colors matching node backgrounds
        st.markdown(
            '<div style="display: flex; justify-content: center; align-items: center; gap: 24px; margin-bottom: 1rem; padding: 12px 20px; background: #f8f9fa; border-radius: 8px; border: 1px solid #e9ecef;">'
            '<div style="display: flex; align-items: center; gap: 8px;">'
            '<span style="width: 14px; height: 14px; background: #E6F5F6; border: 2px solid #007C85; border-radius: 3px; display: inline-block;"></span>'
            '<span style="font-size: 13px; color: #495057; font-weight: 500;">Source</span>'
            '</div>'
            '<div style="display: flex; align-items: center; gap: 8px;">'
            '<span style="width: 16px; height: 16px; background: #FDE8EA; border: 2px solid #E51943; border-radius: 50%; display: inline-block;"></span>'
            '<span style="font-size: 13px; color: #495057; font-weight: 500;">Materialized View</span>'
            '</div>'
            '<div style="display: flex; align-items: center; gap: 8px;">'
            '<span style="width: 14px; height: 14px; background: #FAF0E8; border: 2px solid #C4836A; border-radius: 3px; display: inline-block;"></span>'
            '<span style="font-size: 13px; color: #495057; font-weight: 500;">Target</span>'
            '</div>'
            '<div style="display: flex; align-items: center; gap: 8px;">'
            '<span style="width: 14px; height: 14px; background: #F0EDED; border: 2px solid #B0A8A8; border-radius: 3px; display: inline-block;"></span>'
            '<span style="font-size: 13px; color: #495057; font-weight: 500;">Implicit</span>'
            '</div>'
            '</div>',
            unsafe_allow_html=True,
        )

        # Load supporting data for the graph
        try:
            storage_df = load_storage_metrics(database=database)
        except Exception:
            storage_df = pd.DataFrame()

        try:
            recent_tp = load_recent_throughput(database=database)
        except Exception:
            recent_tp = pd.DataFrame()

        # Detect error MVs
        error_views = set()
        try:
            error_df = load_mv_errors(database=database)
            if error_df is not None and not error_df.empty:
                error_views = set(error_df["view_name"].unique())
        except Exception:
            pass

        # Kafka health
        try:
            kafka_df = load_kafka_consumers(database=database)
        except Exception:
            kafka_df = None

        # Toolbar row: selected element name + clear button
        selected_id = st.session_state.get("lineage_highlight")
        toolbar_left, toolbar_right = st.columns([3, 1])
        with toolbar_left:
            if selected_id and selected_id in lineage.nodes:
                node = lineage.nodes[selected_id]
                st.markdown(
                    f'<div style="font-size: 0.95rem; font-weight: 600; padding: 0.4rem 0;">'
                    f'<span style="opacity: 0.5;">Selected:</span> {node.database}.<span style="color: #E51943;">{node.name}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="font-size: 0.95rem; padding: 0.4rem 0; opacity: 0.4;">Click a node to highlight its connections</div>',
                    unsafe_allow_html=True,
                )
        with toolbar_right:
            clear_disabled = not bool(selected_id and selected_id in lineage.nodes)
            if st.button("Clear highlight", key="clear_highlight", type="secondary",
                         use_container_width=True, disabled=clear_disabled):
                st.session_state.pop("lineage_highlight", None)
                st.session_state["_lineage_skip_result"] = True
                st.rerun()

        # Create two columns: graph on left, side panel on right
        graph_col, panel_col = st.columns([3, 1])

        with graph_col:
            # Render the graph
            selected_id = render_lineage_graph(
                lineage,
                error_views=error_views,
            )

        with panel_col:
            # Side panel for clicked node
            if selected_id and selected_id in lineage.nodes:
                node = lineage.nodes[selected_id]

                # Determine DDL type
                if selected_id in lineage.mv_names:
                    ddl_type = "MATERIALIZED VIEW"
                else:
                    ddl_type = "TABLE"

                # Details section with database and engine
                st.markdown("**Details**")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Database", node.database)
                with col2:
                    st.metric("Engine", node.engine)

                st.markdown("---")

                # Fetch appropriate DDL based on entity type
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

                # Show node details in the side panel
                render_node_detail_sidebar(node.database, node.name, storage_df, create_sql, ddl_type)
            else:
                st.info("Click on a node to view details")

    except Exception as e:
        st.error(f"Failed to load lineage: {e}")


def render_metrics_page(database=None):
    """Data flow metrics page with auto-refreshing charts."""
    st.header("Metrics")

    # Show selected database
    if database and database != "All":
        st.caption(f"Database: **{database}**")

    try:
        with st.spinner("Loading throughput data..."):
            throughput_df = load_throughput(database=database)

        if throughput_df is None or throughput_df.empty:
            st.info("No throughput data available. Materialized views may not have been active recently.")
            return

        render_metrics_cards(throughput_df)
        st.divider()

        mv_names = sorted(throughput_df["view_name"].unique())
        options = ["All"] + mv_names
        selected_mv = st.selectbox("Filter by Materialized View", options)

        # Tabs for charts and errors
        tab_charts, tab_errors, tab_kafka = st.tabs(["Charts", "Errors", "Kafka"])

        with tab_charts:
            _render_charts_fragment(throughput_df, selected_mv, database)

        with tab_errors:
            try:
                error_df = load_mv_errors(database=database)
                render_mv_errors_table(error_df)
            except Exception as e:
                st.error(f"Failed to load MV errors: {e}")

        with tab_kafka:
            try:
                kafka_df = load_kafka_consumers(database=database)
                if kafka_df is not None:
                    render_kafka_consumers(kafka_df)
                else:
                    st.info("No Kafka engine tables found in this cluster.")
            except Exception as e:
                st.error(f"Failed to load Kafka data: {e}")

    except Exception as e:
        st.error(f"Failed to load metrics: {e}")


@st.fragment(run_every=300)
def _render_charts_fragment(throughput_df, selected_mv, database=None):
    """Auto-refreshing chart fragment. Re-fetches throughput data every 30s."""
    try:
        fresh_df = load_throughput(database=database)
        if fresh_df is not None and not fresh_df.empty:
            render_throughput_charts(fresh_df, selected_mv)
        else:
            render_throughput_charts(throughput_df, selected_mv)
    except Exception:
        render_throughput_charts(throughput_df, selected_mv)


def render_tables_page(schema_df, database=None):
    """Tables listing and detail page with storage treemap tab."""
    st.header("Tables")

    # Show selected database
    if database and database != "All":
        st.caption(f"Database: **{database}**")

    if schema_df is None or schema_df.empty:
        st.info("No tables found.")
        return

    tab_list, tab_treemap = st.tabs(["Table List", "Storage Treemap"])

    with tab_list:
        # Full table listing
        display_df = schema_df[["database", "name", "engine", "total_rows", "total_bytes"]].copy()
        display_df["total_rows"] = display_df["total_rows"].apply(format_number)
        display_df["total_bytes"] = display_df["total_bytes"].apply(format_bytes)
        display_df.columns = ["Database", "Name", "Engine", "Rows", "Size"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.divider()

        # Detail selectbox — pre-select from sidebar click
        table_options = [f"{row['database']}.{row['name']}" for _, row in schema_df.iterrows()]
        sel = st.session_state.get("selected_table")
        default_idx = 0
        if sel:
            full_name = f"{sel[0]}.{sel[1]}"
            if full_name in table_options:
                default_idx = table_options.index(full_name)

        chosen = st.selectbox("Select table for details", table_options, index=default_idx)

        if chosen:
            parts = chosen.split(".", 1)
            database, table = parts[0], parts[1]
            try:
                with st.spinner("Loading storage metrics..."):
                    storage_df = load_storage_metrics(database=database)
                render_table_detail(database, table, storage_df)
            except Exception as e:
                st.error(f"Failed to load table detail: {e}")

    with tab_treemap:
        try:
            with st.spinner("Loading partition data..."):
                partition_df = load_partition_storage(database=database)
            render_storage_treemap(partition_df)
        except Exception as e:
            st.error(f"Failed to load storage treemap: {e}")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

success, version_or_error, host = render_connection_status()

# Fetch list of databases if connected
available_databases = []
if success:
    try:
        available_databases = load_databases()
    except Exception:
        available_databases = []

# Get selected database
selected_db = st.session_state.get("selected_database", "All")

# Render sidebar with database selector
render_sidebar_nav(
    connected=success,
    version=version_or_error,
    host=host,
    databases=available_databases,
    selected_db=selected_db
)

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

if success:
    page = st.session_state.get("current_page", "overview")
    selected_db = st.session_state.get("selected_database", "All")

    if page == "overview":
        render_overview_page(database=selected_db)
    elif page == "lineage":
        render_lineage_page(database=selected_db)
    elif page == "metrics":
        render_metrics_page(database=selected_db)
    elif page == "tables":
        with st.spinner("Loading schema..."):
            try:
                schema_df = load_schema(database=selected_db)
            except Exception:
                schema_df = pd.DataFrame()

        render_tables_page(schema_df, database=selected_db)
    else:
        render_overview_page(database=selected_db)

else:
    st.warning(
        "Not connected to ClickHouse. Please check your `.env` configuration.\n\n"
        "Copy `.env.example` to `.env` and fill in your ClickHouse Cloud credentials."
    )
