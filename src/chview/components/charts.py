"""Plotly chart rendering components."""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# F0 design system colors for Plotly (hex – CSS vars don't work in Plotly)
_F0 = {
    "accent": "#E51745",
    "viridian": "#07A4AE",
    "grid": "rgba(13, 21, 37, 0.06)",
}
_F0_CATEGORICAL = [
    "#07A4AE",
    "#5B9CF5",
    "#94BE44",
    "#C76B8A",
    "#F98332",
    "#5758EB",
    "#305C48",
    "#BF8659",
]
_F0_TREEMAP_SCALE = ["#E8F8F7", "#07A4AE"]


def render_throughput_charts(
    throughput_df: pd.DataFrame, selected_mv: Optional[str] = None
) -> None:
    """Render throughput charts using Plotly area charts.

    Args:
        throughput_df: DataFrame with throughput data
        selected_mv: Optional MV name to filter to
    """
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
    st.markdown(
        '<div class="lenses-card-title">Rows per 5-Minute Interval</div>',
        unsafe_allow_html=True,
    )
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=chart_data[time_col],
            y=chart_data["rows_written"],
            name="Rows Written",
            fill="tozeroy",
            fillcolor="rgba(229, 23, 69, 0.15)",
            line={"color": _F0["accent"], "width": 2},
            mode="lines",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=chart_data[time_col],
            y=chart_data["rows_read"],
            name="Rows Read",
            fill="tozeroy",
            fillcolor="rgba(7, 164, 174, 0.15)",
            line={"color": _F0["viridian"], "width": 2},
            mode="lines",
        )
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"t": 10, "b": 40, "l": 60, "r": 10},
        height=300,
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        },
        xaxis={"showgrid": False},
        yaxis={"showgrid": True, "gridcolor": _F0["grid"]},
    )
    st.plotly_chart(fig, use_container_width=True)

    # Duration chart
    st.markdown(
        '<div class="lenses-card-title">Average Duration (ms)</div>',
        unsafe_allow_html=True,
    )
    duration_data = df.groupby(time_col)["avg_duration_ms"].mean().reset_index()
    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(
            x=duration_data[time_col],
            y=duration_data["avg_duration_ms"],
            name="Avg Duration",
            fill="tozeroy",
            fillcolor="rgba(229, 23, 69, 0.08)",
            line={"color": _F0["accent"], "width": 2},
            mode="lines",
        )
    )
    fig2.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"t": 10, "b": 40, "l": 60, "r": 10},
        height=250,
        xaxis={"showgrid": False},
        yaxis={"showgrid": True, "gridcolor": _F0["grid"], "title": "ms"},
    )
    st.plotly_chart(fig2, use_container_width=True)


def render_storage_treemap(partition_df: pd.DataFrame) -> None:
    """Render a Plotly treemap of storage by database > table > partition.

    Args:
        partition_df: DataFrame with partition storage data
    """
    if partition_df is None or partition_df.empty:
        st.info("No partition storage data available.")
        return

    df = partition_df.copy()
    df = df[df["bytes_on_disk"] > 0]
    if df.empty:
        st.info("No storage data to display.")
        return

    fig = px.treemap(
        df,
        path=["database", "table", "partition"],
        values="bytes_on_disk",
        color="bytes_on_disk",
        color_continuous_scale=_F0_TREEMAP_SCALE,
        hover_data={"rows": ":,", "compressed_bytes": ":,"},
    )
    fig.update_layout(
        margin={"t": 30, "b": 10, "l": 10, "r": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        height=500,
        coloraxis_colorbar={"title": "Bytes on Disk"},
    )
    fig.update_traces(
        textinfo="label+value",
        texttemplate="%{label}<br>%{value:,.0f} B",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_engine_pie_chart(schema_df: pd.DataFrame) -> None:
    """Render a donut chart of table engine distribution.

    Args:
        schema_df: DataFrame with schema info including 'engine' column
    """
    if schema_df is None or schema_df.empty:
        st.info("No data for chart.")
        return

    engine_counts = schema_df["engine"].value_counts().reset_index()
    engine_counts.columns = ["Engine", "Count"]
    fig = px.pie(
        engine_counts,
        values="Count",
        names="Engine",
        color_discrete_sequence=_F0_CATEGORICAL,
        hole=0.4,
    )
    fig.update_layout(
        margin={"t": 10, "b": 10, "l": 10, "r": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend={"font": {"size": 11}},
        height=300,
    )
    fig.update_traces(textinfo="label+percent", textfont_size=11)
    st.plotly_chart(fig, use_container_width=True)
