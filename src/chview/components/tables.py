"""DataFrame table rendering components."""

from typing import Optional

import pandas as pd
import streamlit as st

from chview.core.formatters import format_bytes, format_number


def render_metrics_cards(throughput_df: pd.DataFrame) -> None:
    """Render summary metric cards from throughput data.

    Args:
        throughput_df: DataFrame with throughput metrics
    """
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


def render_table_detail(
    database: str, table: str, storage_df: Optional[pd.DataFrame]
) -> None:
    """Render detailed info for a selected table.

    Args:
        database: Database name
        table: Table name
        storage_df: Storage metrics DataFrame
    """
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


def render_node_detail(
    database: str,
    table: str,
    storage_df: Optional[pd.DataFrame],
    create_sql: str,
) -> None:
    """Render a detail panel for a clicked lineage node.

    Args:
        database: Database name
        table: Table/view name
        storage_df: Storage metrics DataFrame
        create_sql: CREATE TABLE/VIEW SQL statement
    """
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


def render_node_detail_sidebar(
    database: str,
    table: str,
    storage_df: Optional[pd.DataFrame],
    create_sql: str,
    ddl_type: str = "TABLE",
) -> None:
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


def render_schema_table(schema_df: pd.DataFrame) -> None:
    """Render the full schema table with formatted rows and sizes.

    Args:
        schema_df: DataFrame with schema info
    """
    if schema_df is None or schema_df.empty:
        st.info("No tables found.")
        return

    display_df = schema_df[
        ["database", "name", "engine", "total_rows", "total_bytes"]
    ].copy()
    display_df["total_rows"] = display_df["total_rows"].apply(format_number)
    display_df["total_bytes"] = display_df["total_bytes"].apply(format_bytes)
    display_df.columns = ["Database", "Name", "Engine", "Rows", "Size"]
    st.dataframe(display_df, use_container_width=True, hide_index=True)
