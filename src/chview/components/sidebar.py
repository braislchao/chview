"""Sidebar navigation and database selector components."""

from typing import Optional

import streamlit as st

from chview.core.formatters import format_number


def render_database_selector(
    databases: list[str], selected_db: str = "All"
) -> str:
    """Render database selector dropdown in sidebar.

    Args:
        databases: List of database names
        selected_db: Currently selected database name

    Returns:
        Selected database name
    """
    options = ["All"] + databases

    st.sidebar.markdown(
        """
    <div style="margin: 0.5rem 0 0.5rem 0;">
        <div style="font-size: 0.75rem; font-weight: 600; color: #666; margin-bottom: 0.3rem;">
            DATABASE
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    selected = st.sidebar.selectbox(
        "Select database",
        options=options,
        index=options.index(selected_db) if selected_db in options else 0,
        key="db_selector",
        label_visibility="collapsed",
    )

    return selected  # type: ignore[return-value]


def render_sidebar_nav(
    connected: bool = False,
    version: str = "",
    host: str = "",
    databases: Optional[list[str]] = None,
    selected_db: str = "All",
) -> str:
    """Render sidebar logo, connection status, refresh, navigation.

    Returns:
        Currently selected database name
    """
    # Logo
    st.sidebar.markdown(
        """
    <div style="text-align: center; padding: 1.2rem 0 0.6rem 0;">
        <div style="font-size: 2.8rem; font-weight: 600; font-family: 'Inter', sans-serif; text-transform: lowercase; letter-spacing: 3px;">
            <span style="color: #E07A8A;">ch</span><span style="font-weight: 300; color: #5A6B7C;">view</span>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Connection status block
    if connected:
        st.sidebar.markdown(
            f'<div style="text-align: center; font-size: 0.85rem; color: #007C85;">'
            f'<span style="font-size: 0.6rem;">&#9679;</span> Connected · v{version}</div>',
            unsafe_allow_html=True,
        )
        st.sidebar.markdown(
            f'<div style="text-align: center; font-size: 0.85rem; opacity: 0.6;">{host}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.sidebar.markdown(
            '<div style="text-align: center; font-size: 0.85rem; color: #E51943;">'
            '<span style="font-size: 0.6rem;">&#9679;</span> Disconnected</div>',
            unsafe_allow_html=True,
        )
        st.sidebar.markdown(
            f'<div style="text-align: center; font-size: 0.85rem; opacity: 0.6;">{host}</div>',
            unsafe_allow_html=True,
        )

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
    if st.sidebar.button(
        "\u27f3 Refresh data",
        key="nav_refresh",
        use_container_width=True,
        type="secondary",
    ):
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
        if st.sidebar.button(
            label, key=f"nav_{key}", use_container_width=True, type=btn_type
        ):
            st.session_state["current_page"] = key
            st.rerun()

    return selected_db


def render_connection_status() -> tuple[bool, str, str]:
    """Test connection and return (success, version, host). Cached in session_state."""
    if "connection_result" not in st.session_state:
        from chview.db.client import ClickHouseClient

        st.session_state["connection_result"] = ClickHouseClient.test_connection()
    return st.session_state["connection_result"]  # type: ignore[return-value]


def render_schema_sidebar(schema_df) -> Optional[tuple[str, str]]:
    """Render the schema browser in the sidebar.

    Returns:
        Selected (database, table) tuple, or None
    """
    if schema_df is None or schema_df.empty:
        st.sidebar.info("No tables found.")
        return None

    st.sidebar.caption("SCHEMA BROWSER")

    databases = schema_df["database"].unique()

    selected = None
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

                if st.button(
                    label, key=f"table_{db}_{row['name']}", use_container_width=True
                ):
                    selected = (db, row["name"])
                    st.session_state["current_page"] = "tables"

    return selected
