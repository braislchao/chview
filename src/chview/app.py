"""CHView Streamlit application entry point.

This module is the thin top-level entry point.  It:
  1. Calls ``st.set_page_config`` (must happen before any other Streamlit call).
  2. Defines all ``@st.cache_data`` loader wrappers.
  3. Initialises session state.
  4. Renders the sidebar (connection status + navigation).
  5. Dispatches to the appropriate page function.
"""

import streamlit as st

st.set_page_config(page_title="CHView", page_icon="📊", layout="wide")

import pandas as pd  # noqa: E402  (must come after set_page_config)

from chview.components.sidebar import render_connection_status, render_sidebar_nav
from chview.components.styles import inject_custom_css
from chview.db.repository import ClickHouseRepository
from chview.pages.lineage import render_lineage_page
from chview.pages.metrics import render_metrics_page
from chview.pages.overview import render_overview_page
from chview.pages.tables import render_tables_page

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

inject_custom_css()

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "overview"

if "selected_database" not in st.session_state:
    st.session_state["selected_database"] = "All"

# ---------------------------------------------------------------------------
# Repository (singleton per process – not cached itself)
# ---------------------------------------------------------------------------

_repo = ClickHouseRepository()

# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------


@st.cache_data(ttl=120)
def load_schema(database=None):
    return _repo.fetch_schema(database)


@st.cache_data(ttl=300)
def load_storage_metrics(database=None):
    return _repo.fetch_storage_metrics(database)


@st.cache_data(ttl=120)
def load_materialized_views(database=None):
    return _repo.fetch_materialized_views(database)


@st.cache_data(ttl=300)
def load_throughput(hours=24, database=None):
    return _repo.fetch_mv_throughput(hours, database)


@st.cache_data(ttl=120)
def load_cluster_info(database=None):
    return _repo.fetch_cluster_info(database)


@st.cache_data(ttl=300)
def load_partition_storage(database=None):
    return _repo.fetch_partition_storage(database)


@st.cache_data(ttl=300)
def load_recent_throughput(minutes=30, database=None):
    return _repo.fetch_recent_throughput(minutes, database)


@st.cache_data(ttl=60)
def load_mv_errors(hours=24, database=None):
    return _repo.fetch_mv_errors(hours, database)


@st.cache_data(ttl=60)
def load_kafka_consumers(database=None):
    return _repo.fetch_kafka_consumers(database)


@st.cache_data(ttl=120)
def load_create_table(database, table):
    return _repo.fetch_create_table(database, table)


@st.cache_data(ttl=120)
def load_create_view(database, view):
    return _repo.fetch_create_view(database, view)


@st.cache_data(ttl=300)
def load_databases():
    return _repo.fetch_databases()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

success, version_or_error, host = render_connection_status()

available_databases: list[str] = []
if success:
    try:
        available_databases = load_databases()
    except Exception:
        available_databases = []

selected_db = st.session_state.get("selected_database", "All")

render_sidebar_nav(
    connected=success,
    version=version_or_error,
    host=host,
    databases=available_databases,
    selected_db=selected_db,
)

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

if success:
    page = st.session_state.get("current_page", "overview")
    selected_db = st.session_state.get("selected_database", "All")

    if page == "overview":
        render_overview_page(
            load_cluster_info=load_cluster_info,
            load_schema=load_schema,
            load_mv_errors=load_mv_errors,
            database=selected_db,
        )
    elif page == "lineage":
        render_lineage_page(
            load_materialized_views=load_materialized_views,
            load_schema=load_schema,
            load_storage_metrics=load_storage_metrics,
            load_recent_throughput=load_recent_throughput,
            load_mv_errors=load_mv_errors,
            load_kafka_consumers=load_kafka_consumers,
            load_create_table=load_create_table,
            load_create_view=load_create_view,
            database=selected_db,
        )
    elif page == "metrics":
        render_metrics_page(
            load_throughput=load_throughput,
            load_mv_errors=load_mv_errors,
            load_kafka_consumers=load_kafka_consumers,
            database=selected_db,
        )
    elif page == "tables":
        with st.spinner("Loading schema..."):
            try:
                schema_df = load_schema(database=selected_db)
            except Exception:
                schema_df = pd.DataFrame()

        render_tables_page(
            schema_df=schema_df,
            load_storage_metrics=load_storage_metrics,
            load_partition_storage=load_partition_storage,
            database=selected_db,
        )
    else:
        render_overview_page(
            load_cluster_info=load_cluster_info,
            load_schema=load_schema,
            load_mv_errors=load_mv_errors,
            database=selected_db,
        )

else:
    st.warning(
        "Not connected to ClickHouse. Please check your `.env` configuration.\n\n"
        "Copy `.env.example` to `.env` and fill in your ClickHouse Cloud credentials."
    )
