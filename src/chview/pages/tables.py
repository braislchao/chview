"""Tables page: schema listing, table detail, and storage treemap."""

from typing import Optional

import pandas as pd
import streamlit as st

from chview.components.charts import render_storage_treemap
from chview.components.tables import render_table_detail
from chview.core.formatters import format_bytes, format_number


def render_tables_page(
    schema_df: Optional[pd.DataFrame],
    load_storage_metrics,
    load_partition_storage,
    database: Optional[str] = None,
) -> None:
    """Render the tables listing and detail page with storage treemap tab.

    Args:
        schema_df: Pre-loaded schema DataFrame
        load_storage_metrics: Cached storage metrics loader
        load_partition_storage: Cached partition storage loader
        database: Currently selected database filter
    """
    st.header("Tables")

    if database and database != "All":
        st.caption(f"Database: **{database}**")

    if schema_df is None or schema_df.empty:
        st.info("No tables found.")
        return

    tab_list, tab_treemap = st.tabs(["Table List", "Storage Treemap"])

    with tab_list:
        display_df = schema_df[
            ["database", "name", "engine", "total_rows", "total_bytes"]
        ].copy()
        display_df["total_rows"] = display_df["total_rows"].apply(format_number)
        display_df["total_bytes"] = display_df["total_bytes"].apply(format_bytes)
        display_df.columns = ["Database", "Name", "Engine", "Rows", "Size"]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.divider()

        # Detail selectbox — pre-select from sidebar click
        table_options = [
            f"{row['database']}.{row['name']}" for _, row in schema_df.iterrows()
        ]
        sel = st.session_state.get("selected_table")
        default_idx = 0
        if sel:
            full_name = f"{sel[0]}.{sel[1]}"
            if full_name in table_options:
                default_idx = table_options.index(full_name)

        chosen = st.selectbox(
            "Select table for details", table_options, index=default_idx
        )

        if chosen:
            parts = chosen.split(".", 1)
            tbl_database, tbl_table = parts[0], parts[1]
            try:
                with st.spinner("Loading storage metrics..."):
                    storage_df = load_storage_metrics(database=tbl_database)
                render_table_detail(tbl_database, tbl_table, storage_df)
            except Exception as e:
                st.error(f"Failed to load table detail: {e}")

    with tab_treemap:
        try:
            with st.spinner("Loading partition data..."):
                partition_df = load_partition_storage(database=database)
            render_storage_treemap(partition_df)
        except Exception as e:
            st.error(f"Failed to load storage treemap: {e}")
