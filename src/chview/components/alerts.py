"""Health banners and error alert components."""

from typing import Optional

import pandas as pd
import streamlit as st


def render_mv_health_banner(error_df: Optional[pd.DataFrame]) -> None:
    """Render an alert banner showing MV health status.

    Args:
        error_df: DataFrame with MV error data, or None/empty if healthy
    """
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
        f"</div>",
        unsafe_allow_html=True,
    )


def render_mv_errors_table(error_df: Optional[pd.DataFrame]) -> None:
    """Render a detailed table of MV errors.

    Args:
        error_df: DataFrame with MV error data
    """
    if error_df is None or error_df.empty:
        st.info("No MV errors in the selected time window.")
        return

    display_df = error_df[["view_name", "exception_code", "exception", "event_time"]].copy()
    display_df.columns = ["View", "Code", "Exception", "Time"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_kafka_consumers(kafka_df: Optional[pd.DataFrame]) -> None:
    """Render Kafka consumer health indicators.

    Args:
        kafka_df: DataFrame with Kafka consumer data
    """
    if kafka_df is None or kafka_df.empty:
        return

    from chview.core.formatters import format_number

    st.subheader("Kafka Consumer Health")

    display_df = kafka_df.copy()

    def _health_status(row: pd.Series) -> str:
        if not row.get("is_currently_used", True):
            return "Inactive"
        secs = row.get("seconds_since_poll", 0)
        if secs > 300:
            return "Error"
        if secs > 60:
            return "Warning"
        return "Healthy"

    display_df["status"] = display_df.apply(_health_status, axis=1)

    def _status_color(status: str) -> str:
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
                st.markdown(
                    f"**Status:** <span style='color:{color}'>{status}</span>",
                    unsafe_allow_html=True,
                )
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
