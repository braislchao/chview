"""Overview page: cluster metrics and table listing."""

from typing import Optional

import streamlit as st

from chview.components.alerts import render_mv_health_banner
from chview.components.charts import render_engine_pie_chart
from chview.core.formatters import format_bytes, format_number


def render_overview_page(
    load_cluster_info,
    load_schema,
    load_mv_errors,
    database: Optional[str] = None,
) -> None:
    """Render the overview dashboard with cluster metrics and table listing.

    Args:
        load_cluster_info: Cached loader for cluster info dict
        load_schema: Cached loader for schema DataFrame
        load_mv_errors: Cached loader for MV errors DataFrame
        database: Currently selected database filter
    """
    st.header("Overview")

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

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown(
            '<div class="lenses-card-title">Table Overview</div>',
            unsafe_allow_html=True,
        )
        try:
            schema_df = load_schema(database=database)
            if schema_df is not None and not schema_df.empty:
                display_df = schema_df[
                    ["database", "name", "engine", "total_rows", "total_bytes"]
                ].copy()
                display_df["total_rows"] = display_df["total_rows"].apply(format_number)
                display_df["total_bytes"] = display_df["total_bytes"].apply(
                    format_bytes
                )
                display_df.columns = ["Database", "Name", "Engine", "Rows", "Size"]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No tables found.")
        except Exception as e:
            st.error(f"Failed to load schema: {e}")

    with col_right:
        st.markdown(
            '<div class="lenses-card-title">Engine Breakdown</div>',
            unsafe_allow_html=True,
        )
        try:
            schema_df = load_schema(database=database)
            render_engine_pie_chart(schema_df)
        except Exception as e:
            st.error(f"Failed to load engine breakdown: {e}")
