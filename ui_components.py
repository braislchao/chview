import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

def inject_custom_css():
    """Inject global custom CSS for a clean, theme-respecting UI."""
    st.markdown("""
    <style>
    /* --- Factorial Design System Palette --- */
    :root {
        --radical-red: #E51943;
        --viridian-green: #007C85;
        --ebony-clay: #25253D;
        --red-light: #FDE8EA;
        --viridian-light: #E6F5F6;
        --warning: #E5A019;
        --warning-light: #FFF5E0;
        --error-light: #FDE8EA;
        --success-light: #E6F5F6;
    }

    /* --- Fonts --- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    code, pre, .stCode {
        font-family: 'JetBrains Mono', monospace;
    }

    /* --- Hide Streamlit footer --- */
    footer {visibility: hidden;}

    /* --- Match header background to sidebar --- */
    header[data-testid="stHeader"] {
        background: var(--secondary-background-color) !important;
    }

    /* --- Top padding to clear Streamlit header --- */
    .block-container {
        padding-top: 3rem !important;
    }

    /* --- Nav buttons in sidebar --- */
    .stSidebar button[kind="primary"] {
        background: rgba(229, 25, 67, 0.10) !important;
        border: none !important;
        border-radius: 6px !important;
        color: var(--radical-red) !important;
        text-align: center !important;
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"] {
        background: transparent !important;
        border: none !important;
        border-radius: 6px !important;
        opacity: 0.6;
        text-align: center !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        padding: 0.5rem 0.8rem !important;
    }
    .stSidebar button[kind="secondary"]:hover {
        opacity: 1;
        background: rgba(229, 25, 67, 0.08) !important;
    }

    /* --- Card title --- */
    .lenses-card-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--ebony-clay);
        opacity: 0.6;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }

    /* --- Metric cards --- */
    div[data-testid="stMetric"] {
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 8px;
        padding: 1rem 1.25rem;
    }
    div[data-testid="stMetric"] label {
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        opacity: 0.6;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
        font-weight: 600 !important;
    }

    /* --- Dataframe borders --- */
    .stDataFrame {
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 8px;
    }

    /* --- Expander headers in sidebar --- */
    .stSidebar details summary {
        font-weight: 600 !important;
        font-size: 0.85rem !important;
    }

    /* --- Alert banners --- */
    .chview-alert-healthy {
        background: var(--viridian-light);
        border-left: 4px solid var(--viridian-green);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }
    .chview-alert-error {
        background: var(--red-light);
        border-left: 4px solid var(--radical-red);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }
    .chview-alert-warning {
        background: var(--warning-light);
        border-left: 4px solid var(--warning);
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: var(--ebony-clay);
        font-size: 0.9rem;
    }

    /* --- Streamlit Flow container (parent-level only; component internals
         are in an iframe and styled via inline node styles) --- */
    iframe[title="streamlit_flow.streamlit_flow"] {
        border: none !important;
        height: calc(100vh - 320px) !important;
        min-height: 400px;
    }
    </style>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

def format_number(n):
    """Format a number in human-readable form (1.2K, 3.4M, etc.)."""
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "0"
    n = float(n)
    if abs(n) < 1_000:
        return f"{n:,.0f}"
    for unit in ["", "K", "M", "B", "T"]:
        if abs(n) < 1_000:
            return f"{n:,.1f}{unit}"
        n /= 1_000
    return f"{n:,.1f}P"


def format_bytes(n):
    """Format bytes in human-readable form."""
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "0 B"
    n = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# Keep private alias for backwards compat within this module
_format_bytes = format_bytes


# ---------------------------------------------------------------------------
# Sidebar Navigation
# ---------------------------------------------------------------------------

def render_database_selector(databases, selected_db="All"):
    """Render database selector dropdown in sidebar.

    Args:
        databases: List of database names
        selected_db: Currently selected database name

    Returns:
        Selected database name
    """
    options = ["All"] + databases

    st.sidebar.markdown("""
    <div style="margin: 0.5rem 0 0.5rem 0;">
        <div style="font-size: 0.75rem; font-weight: 600; color: #666; margin-bottom: 0.3rem;">
            DATABASE
        </div>
    </div>
    """, unsafe_allow_html=True)

    selected = st.sidebar.selectbox(
        "Select database",
        options=options,
        index=options.index(selected_db) if selected_db in options else 0,
        key="db_selector",
        label_visibility="collapsed"
    )

    return selected


def render_sidebar_nav(connected=False, version="", host="", databases=None, selected_db="All"):
    """Render sidebar logo, connection status, refresh, navigation."""
    # Logo
    st.sidebar.markdown("""
    <div style="text-align: center; padding: 1.2rem 0 0.6rem 0;">
        <div style="font-size: 2.8rem; font-weight: 600; font-family: 'Inter', sans-serif; text-transform: lowercase; letter-spacing: 3px;">
            <span style="color: #E07A8A;">ch</span><span style="font-weight: 300; color: #5A6B7C;">view</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Connection status block
    if connected:
        st.sidebar.markdown(
            f'<div style="text-align: center; font-size: 0.85rem; color: #007C85;">'
            f'<span style="font-size: 0.6rem;">&#9679;</span> Connected · v{version}</div>',
            unsafe_allow_html=True,
        )
        st.sidebar.markdown(f'<div style="text-align: center; font-size: 0.85rem; opacity: 0.6;">{host}</div>', unsafe_allow_html=True)
    else:
        st.sidebar.markdown(
            f'<div style="text-align: center; font-size: 0.85rem; color: #E51943;">'
            f'<span style="font-size: 0.6rem;">&#9679;</span> Disconnected</div>',
            unsafe_allow_html=True,
        )
        st.sidebar.markdown(f'<div style="text-align: center; font-size: 0.85rem; opacity: 0.6;">{host}</div>', unsafe_allow_html=True)

    # Database selector (only if connected and databases available)
    if connected and databases:
        st.sidebar.divider()
        selected = render_database_selector(databases, selected_db)
        if selected != selected_db:
            st.session_state["selected_database"] = selected
            st.cache_data.clear()
            st.rerun()
        selected_db = selected

    # Refresh — right under connection info
    if st.sidebar.button("\u27f3 Refresh data", key="nav_refresh", use_container_width=True, type="secondary"):
        st.cache_data.clear()
        st.session_state.pop("connection_result", None)
        st.rerun()

    st.sidebar.divider()

    # Navigation
    pages = [
        ("overview", "Overview"),
        ("lineage", "Lineage"),
        ("metrics", "Metrics"),
        ("tables", "Tables"),
    ]

    current = st.session_state.get("current_page", "overview")

    for key, label in pages:
        btn_type = "primary" if current == key else "secondary"
        if st.sidebar.button(label, key=f"nav_{key}", use_container_width=True, type=btn_type):
            st.session_state["current_page"] = key
            st.rerun()

    return selected_db


def render_connection_status():
    """Test connection and return (success, version, host). Cached in session_state."""
    # Cache result to avoid re-querying on every Streamlit rerun
    # (e.g. when streamlit_flow triggers a component rerun)
    if "connection_result" not in st.session_state:
        from db import test_connection
        st.session_state["connection_result"] = test_connection()
    return st.session_state["connection_result"]


# ---------------------------------------------------------------------------
# Schema Browser
# ---------------------------------------------------------------------------

def render_schema_sidebar(schema_df):
    """Render the schema browser in the sidebar. Returns selected (database, table) or None."""
    selected = None

    if schema_df is None or schema_df.empty:
        st.sidebar.info("No tables found.")
        return None

    st.sidebar.caption("SCHEMA BROWSER")

    databases = schema_df["database"].unique()

    for db in sorted(databases):
        db_tables = schema_df[schema_df["database"] == db]
        with st.sidebar.expander(f"{db} ({len(db_tables)})", expanded=False):
            for _, row in db_tables.iterrows():
                engine = row["engine"]
                total_rows = row["total_rows"]
                row_display = format_number(total_rows) if total_rows is not None else "N/A"

                # Icon by engine type
                if engine == "MaterializedView":
                    icon = "\u25c6"
                elif "MergeTree" in engine:
                    icon = "\u25a0"
                else:
                    icon = "\u25cb"

                label = f"{icon}  {row['name']}  \u00b7  {row_display} rows"

                if st.button(label, key=f"table_{db}_{row['name']}", use_container_width=True):
                    selected = (db, row["name"])
                    st.session_state["current_page"] = "tables"

    return selected


# ---------------------------------------------------------------------------
# Table Detail
# ---------------------------------------------------------------------------

def render_table_detail(database, table, storage_df):
    """Render detailed info for a selected table."""
    st.subheader(f"`{database}`.`{table}`")

    if storage_df is None or storage_df.empty:
        st.info("No storage metrics available for this table.")
        return

    table_data = storage_df[
        (storage_df["database"] == database) & (storage_df["table"] == table)
    ]

    if table_data.empty:
        st.info("No active parts found for this table.")
        return

    row = table_data.iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Rows", format_number(row.get("rows")))
    with col2:
        st.metric("Disk Usage", format_bytes(row.get("bytes_on_disk")))
    with col3:
        st.metric("Compressed", format_bytes(row.get("compressed_bytes")))
    with col4:
        st.metric("Uncompressed", format_bytes(row.get("uncompressed_bytes")))

    if row.get("uncompressed_bytes") and row.get("compressed_bytes"):
        uncomp = float(row["uncompressed_bytes"])
        comp = float(row["compressed_bytes"])
        if comp > 0:
            ratio = uncomp / comp
            st.caption(f"Compression ratio: {ratio:.1f}x")


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def render_metrics_cards(throughput_df):
    """Render summary metric cards from throughput data."""
    if throughput_df is None or throughput_df.empty:
        st.info("No throughput data available.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        total_exec = throughput_df["executions"].sum()
        st.metric("Total Executions", format_number(total_exec))
    with col2:
        total_written = throughput_df["rows_written"].sum()
        st.metric("Total Rows Written", format_number(total_written))
    with col3:
        avg_dur = throughput_df["avg_duration_ms"].mean()
        st.metric("Avg Duration", f"{avg_dur:.1f} ms" if pd.notna(avg_dur) else "N/A")


def render_throughput_charts(throughput_df, selected_mv=None):
    """Render throughput charts using Plotly area charts. Optionally filter to a specific MV."""
    if throughput_df is None or throughput_df.empty:
        st.info("No throughput data available for charts.")
        return

    df = throughput_df.copy()
    if selected_mv and selected_mv != "All":
        df = df[df["view_name"] == selected_mv]

    if df.empty:
        st.info(f"No data for {selected_mv}.")
        return

    time_col = "interval_start" if "interval_start" in df.columns else "hour"
    if time_col not in df.columns:
        st.info("No time series data available.")
        return

    chart_data = df.groupby(time_col)[["rows_written", "rows_read"]].sum().reset_index()

    # Rows written/read area chart
    st.markdown('<div class="lenses-card-title">Rows per 5-Minute Interval</div>', unsafe_allow_html=True)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=chart_data[time_col],
        y=chart_data["rows_written"],
        name="Rows Written",
        fill="tozeroy",
        fillcolor="rgba(229, 25, 67, 0.15)",
        line=dict(color="#E51943", width=2),
        mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=chart_data[time_col],
        y=chart_data["rows_read"],
        name="Rows Read",
        fill="tozeroy",
        fillcolor="rgba(0, 124, 133, 0.15)",
        line=dict(color="#007C85", width=2),
        mode="lines",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=40, l=60, r=10),
        height=300,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.1)"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Duration chart
    st.markdown('<div class="lenses-card-title">Average Duration (ms)</div>', unsafe_allow_html=True)
    duration_data = df.groupby(time_col)["avg_duration_ms"].mean().reset_index()
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=duration_data[time_col],
        y=duration_data["avg_duration_ms"],
        name="Avg Duration",
        fill="tozeroy",
        fillcolor="rgba(229, 25, 67, 0.08)",
        line=dict(color="#E51943", width=2),
        mode="lines",
    ))
    fig2.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=40, l=60, r=10),
        height=250,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.1)", title="ms"),
    )
    st.plotly_chart(fig2, use_container_width=True)


# ---------------------------------------------------------------------------
# Storage Treemap
# ---------------------------------------------------------------------------

def render_storage_treemap(partition_df):
    """Render a Plotly treemap of storage by database > table > partition."""
    if partition_df is None or partition_df.empty:
        st.info("No partition storage data available.")
        return

    df = partition_df.copy()
    # Filter out zero-size entries
    df = df[df["bytes_on_disk"] > 0]
    if df.empty:
        st.info("No storage data to display.")
        return

    fig = px.treemap(
        df,
        path=["database", "table", "partition"],
        values="bytes_on_disk",
        color="bytes_on_disk",
        color_continuous_scale=["#E6F5F6", "#007C85"],
        hover_data={"rows": ":,", "compressed_bytes": ":,"},
    )
    fig.update_layout(
        margin=dict(t=30, b=10, l=10, r=10),
        paper_bgcolor="rgba(0,0,0,0)",
        height=500,
        coloraxis_colorbar=dict(title="Bytes on Disk"),
    )
    fig.update_traces(
        textinfo="label+value",
        texttemplate="%{label}<br>%{value:,.0f} B",
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# MV Health Alerts
# ---------------------------------------------------------------------------

def render_mv_health_banner(error_df):
    """Render an alert banner showing MV health status."""
    if error_df is None or error_df.empty:
        st.markdown(
            '<div class="chview-alert-healthy">All Materialized Views healthy</div>',
            unsafe_allow_html=True,
        )
        return

    error_count = error_df["view_name"].nunique()
    st.markdown(
        f'<div class="chview-alert-error">'
        f'{error_count} MV{"s" if error_count != 1 else ""} with errors in the last 24h'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_mv_errors_table(error_df):
    """Render a detailed table of MV errors."""
    if error_df is None or error_df.empty:
        st.info("No MV errors in the selected time window.")
        return

    display_df = error_df[["view_name", "exception_code", "exception", "event_time"]].copy()
    display_df.columns = ["View", "Code", "Exception", "Time"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Kafka Consumer Health
# ---------------------------------------------------------------------------

def render_kafka_consumers(kafka_df):
    """Render Kafka consumer health indicators."""
    if kafka_df is None or kafka_df.empty:
        return

    st.subheader("Kafka Consumer Health")

    display_df = kafka_df.copy()

    def _health_status(row):
        if not row.get("is_currently_used", True):
            return "Inactive"
        secs = row.get("seconds_since_poll", 0)
        if secs > 300:
            return "Error"
        if secs > 60:
            return "Warning"
        return "Healthy"

    display_df["status"] = display_df.apply(_health_status, axis=1)

    # Color-coded status column
    def _status_color(status):
        return {
            "Healthy": "#007C85",
            "Warning": "#E5A019",
            "Error": "#E51943",
            "Inactive": "#B0A8A8",
        }.get(status, "#B0A8A8")

    for _, row in display_df.iterrows():
        table_name = f"{row['database']}.{row['table']}"
        status = row["status"]
        color = _status_color(status)

        with st.expander(f"{table_name} — {row.get('topic', 'N/A')} [{status}]"):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.markdown(f"**Status:** <span style='color:{color}'>{status}</span>", unsafe_allow_html=True)
            with c2:
                st.metric("Messages Read", format_number(row.get("num_messages_read", 0)))
            with c3:
                st.metric("Rebalances", int(row.get("rebalance_count", 0)))
            with c4:
                secs = row.get("seconds_since_poll", 0)
                if secs < 60:
                    st.metric("Last Poll", f"{secs:.0f}s ago")
                elif secs < 3600:
                    st.metric("Last Poll", f"{secs / 60:.1f}m ago")
                else:
                    st.metric("Last Poll", f"{secs / 3600:.1f}h ago")


# ---------------------------------------------------------------------------
# Node Detail Panel (for lineage click-to-query)
# ---------------------------------------------------------------------------

def render_node_detail(database, table, storage_df, create_sql):
    """Render a detail panel for a clicked lineage node."""
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.markdown(f"**`{database}`.`{table}`**")

        if storage_df is not None and not storage_df.empty:
            table_data = storage_df[
                (storage_df["database"] == database) & (storage_df["table"] == table)
            ]
            if not table_data.empty:
                row = table_data.iloc[0]
                st.metric("Rows", format_number(row.get("rows")))
                st.metric("Disk", format_bytes(row.get("bytes_on_disk")))
                comp = row.get("compressed_bytes", 0)
                uncomp = row.get("uncompressed_bytes", 0)
                if comp and uncomp and float(comp) > 0:
                    st.metric("Compression", f"{float(uncomp) / float(comp):.1f}x")
            else:
                st.caption("No storage data available.")
        else:
            st.caption("No storage data available.")

    with col_right:
        st.markdown("**CREATE TABLE**")
        st.code(create_sql, language="sql")


def render_node_detail_sidebar(database, table, storage_df, create_sql, ddl_type="TABLE"):
    """Render a compact detail panel for the sidebar when a lineage node is clicked.

    Args:
        database: Database name
        table: Table/view name
        storage_df: Storage metrics DataFrame
        create_sql: CREATE TABLE/VIEW SQL statement
        ddl_type: Type of DDL - "TABLE" or "VIEW"
    """
    # Storage metrics
    if storage_df is not None and not storage_df.empty:
        table_data = storage_df[
            (storage_df["database"] == database) & (storage_df["table"] == table)
        ]
        if not table_data.empty:
            row = table_data.iloc[0]

            st.markdown("**Storage**")
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Rows", format_number(row.get("rows")))
            with c2:
                st.metric("Disk", format_bytes(row.get("bytes_on_disk")))

            comp = row.get("compressed_bytes", 0)
            uncomp = row.get("uncompressed_bytes", 0)
            if comp and uncomp and float(comp) > 0:
                ratio = float(uncomp) / float(comp)
                st.metric("Compression", f"{ratio:.1f}x")
        else:
            st.caption("No storage data")
    else:
        st.caption("No storage data")

    st.markdown("---")

    # CREATE SQL in an expander to save space
    expander_label = f"CREATE {ddl_type} DDL"
    with st.expander(expander_label, expanded=False):
        st.code(create_sql, language="sql")
