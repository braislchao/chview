"""Metrics page: MV throughput charts, errors, and Kafka health."""

from typing import Optional

import streamlit as st

from chview.components.alerts import render_kafka_consumers, render_mv_errors_table
from chview.components.charts import render_throughput_charts
from chview.components.tables import render_metrics_cards


def render_metrics_page(
    load_throughput,
    load_mv_errors,
    load_kafka_consumers,
    database: Optional[str] = None,
) -> None:
    """Render the data flow metrics page.

    Args:
        load_throughput: Cached MV throughput loader
        load_mv_errors: Cached MV errors loader
        load_kafka_consumers: Cached Kafka consumers loader
        database: Currently selected database filter
    """
    st.header("Metrics")

    if database and database != "All":
        st.caption(f"Database: **{database}**")

    try:
        with st.spinner("Loading throughput data..."):
            throughput_df = load_throughput(database=database)

        if throughput_df is None or throughput_df.empty:
            st.info(
                "No throughput data available. "
                "Materialized views may not have been active recently."
            )
            return

        render_metrics_cards(throughput_df)
        st.divider()

        mv_names = sorted(throughput_df["view_name"].unique())
        options = ["All"] + list(mv_names)
        selected_mv = st.selectbox("Filter by Materialized View", options)

        tab_charts, tab_errors, tab_kafka = st.tabs(["Charts", "Errors", "Kafka"])

        with tab_charts:
            _render_charts_fragment(
                throughput_df, selected_mv, load_throughput, database
            )

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
def _render_charts_fragment(
    throughput_df,
    selected_mv: Optional[str],
    load_throughput,
    database: Optional[str] = None,
) -> None:
    """Auto-refreshing chart fragment. Re-fetches throughput data every 5 minutes."""
    try:
        fresh_df = load_throughput(database=database)
        if fresh_df is not None and not fresh_df.empty:
            render_throughput_charts(fresh_df, selected_mv)
        else:
            render_throughput_charts(throughput_df, selected_mv)
    except Exception:
        render_throughput_charts(throughput_df, selected_mv)
