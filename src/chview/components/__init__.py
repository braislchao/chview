"""UI components public API."""

from chview.components.alerts import (
    render_kafka_consumers,
    render_mv_errors_table,
    render_mv_health_banner,
)
from chview.components.charts import (
    render_engine_pie_chart,
    render_storage_treemap,
    render_throughput_charts,
)
from chview.components.sidebar import (
    render_connection_status,
    render_database_selector,
    render_schema_sidebar,
    render_sidebar_nav,
)
from chview.components.styles import inject_custom_css
from chview.components.tables import (
    render_metrics_cards,
    render_node_detail,
    render_node_detail_sidebar,
    render_schema_table,
    render_table_detail,
)

__all__ = [
    "inject_custom_css",
    "render_connection_status",
    "render_database_selector",
    "render_engine_pie_chart",
    "render_kafka_consumers",
    "render_metrics_cards",
    "render_mv_errors_table",
    "render_mv_health_banner",
    "render_node_detail",
    "render_node_detail_sidebar",
    "render_schema_sidebar",
    "render_schema_table",
    "render_sidebar_nav",
    "render_storage_treemap",
    "render_table_detail",
    "render_throughput_charts",
]
