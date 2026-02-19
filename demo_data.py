"""Synthetic demo data for CHView demo mode.

Each function returns a DataFrame (or dict) matching the schema of its
corresponding ``fetch_*`` counterpart in ``db.py``, using hardcoded data
that mirrors the ``demo_setup.sql`` tables.
"""

from datetime import datetime, timedelta

import pandas as pd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now().replace(second=0, microsecond=0)


def _ts(minutes_ago):
    return _NOW - timedelta(minutes=minutes_ago)


# ---------------------------------------------------------------------------
# demo_cluster_info  ->  fetch_cluster_info
# ---------------------------------------------------------------------------

def demo_cluster_info():
    return {
        "version": "24.3.1.2672 (demo)",
        "uptime_seconds": 86400 * 3 + 7200,
        "user_databases": 2,
        "user_tables": 22,
        "mv_count": 9,
        "total_disk_bytes": 48_500_000,
    }


# ---------------------------------------------------------------------------
# demo_schema  ->  fetch_schema
# ---------------------------------------------------------------------------

_TABLES = [
    ("ecommerce", "raw_events", "MergeTree", 850_000, 34_000_000),
    ("ecommerce", "pageviews_hourly", "SummingMergeTree", 12_400, 620_000),
    ("ecommerce", "mv_pageviews_hourly", "MaterializedView", None, 0),
    ("ecommerce", "revenue_hourly", "SummingMergeTree", 8_200, 410_000),
    ("ecommerce", "mv_revenue_hourly", "MaterializedView", None, 0),
    ("ecommerce", "cart_abandonment", "SummingMergeTree", 6_100, 305_000),
    ("ecommerce", "mv_cart_abandonment", "MaterializedView", None, 0),
    ("ecommerce", "top_products", "ReplacingMergeTree", 200, 18_000),
    ("ecommerce", "mv_top_products", "MaterializedView", None, 0),
    ("analytics", "user_sessions", "ReplacingMergeTree", 42_000, 3_200_000),
    ("analytics", "mv_user_sessions", "MaterializedView", None, 0),
    ("analytics", "search_metrics", "SummingMergeTree", 3_800, 190_000),
    ("analytics", "mv_search_metrics", "MaterializedView", None, 0),
    ("analytics", "refund_tracking", "SummingMergeTree", 1_200, 60_000),
    ("analytics", "mv_refund_tracking", "MaterializedView", None, 0),
    ("analytics", "daily_country_stats", "SummingMergeTree", 900, 45_000),
    ("analytics", "mv_daily_country_stats", "MaterializedView", None, 0),
    ("analytics", "revenue_alerts", "MergeTree", 340, 17_000),
    ("analytics", "mv_revenue_alerts", "MaterializedView", None, 0),
]


def demo_schema():
    return pd.DataFrame(
        _TABLES,
        columns=["database", "name", "engine", "total_rows", "total_bytes"],
    )


# ---------------------------------------------------------------------------
# demo_materialized_views  ->  fetch_materialized_views
# ---------------------------------------------------------------------------

_MVS = [
    ("ecommerce", "mv_pageviews_hourly",
     "CREATE MATERIALIZED VIEW ecommerce.mv_pageviews_hourly TO ecommerce.pageviews_hourly AS SELECT toStartOfHour(event_time) AS hour, country, device, count() AS views, uniqExact(user_id) AS unique_users FROM ecommerce.raw_events WHERE event_type = 'page_view' GROUP BY hour, country, device",
     ["ecommerce"], ["pageviews_hourly"]),
    ("ecommerce", "mv_revenue_hourly",
     "CREATE MATERIALIZED VIEW ecommerce.mv_revenue_hourly TO ecommerce.revenue_hourly AS SELECT toStartOfHour(event_time) AS hour, category, sum(price * quantity) AS revenue, count() AS orders, sum(quantity) AS items_sold FROM ecommerce.raw_events WHERE event_type = 'purchase' GROUP BY hour, category",
     ["ecommerce"], ["revenue_hourly"]),
    ("ecommerce", "mv_cart_abandonment",
     "CREATE MATERIALIZED VIEW ecommerce.mv_cart_abandonment TO ecommerce.cart_abandonment AS SELECT toStartOfHour(event_time) AS hour, category, countIf(event_type = 'add_to_cart') AS carts_created, countIf(event_type = 'purchase') AS carts_purchased FROM ecommerce.raw_events WHERE event_type IN ('add_to_cart', 'purchase') GROUP BY hour, category",
     ["ecommerce"], ["cart_abandonment"]),
    ("ecommerce", "mv_top_products",
     "CREATE MATERIALIZED VIEW ecommerce.mv_top_products TO ecommerce.top_products AS SELECT product_id, any(category) AS category, sum(price * quantity) AS total_revenue, count() AS total_orders, max(event_time) AS last_sold FROM ecommerce.raw_events WHERE event_type = 'purchase' GROUP BY product_id",
     ["ecommerce"], ["top_products"]),
    ("analytics", "mv_user_sessions",
     "CREATE MATERIALIZED VIEW analytics.mv_user_sessions TO analytics.user_sessions AS SELECT session_id, any(user_id) AS user_id, min(event_time) AS session_start, any(country) AS country, any(device) AS device, any(referrer) AS referrer, countIf(event_type = 'page_view') AS page_views, count() AS events FROM ecommerce.raw_events GROUP BY session_id",
     ["analytics"], ["user_sessions"]),
    ("analytics", "mv_search_metrics",
     "CREATE MATERIALIZED VIEW analytics.mv_search_metrics TO analytics.search_metrics AS SELECT toStartOfHour(event_time) AS hour, category, count() AS searches, uniqExact(user_id) AS unique_searchers FROM ecommerce.raw_events WHERE event_type = 'search' GROUP BY hour, category",
     ["analytics"], ["search_metrics"]),
    ("analytics", "mv_refund_tracking",
     "CREATE MATERIALIZED VIEW analytics.mv_refund_tracking TO analytics.refund_tracking AS SELECT toStartOfHour(event_time) AS hour, category, count() AS refunds, sum(price * quantity) AS refund_amount FROM ecommerce.raw_events WHERE event_type = 'refund' GROUP BY hour, category",
     ["analytics"], ["refund_tracking"]),
    ("analytics", "mv_daily_country_stats",
     "CREATE MATERIALIZED VIEW analytics.mv_daily_country_stats TO analytics.daily_country_stats AS SELECT toDate(hour) AS day, country, sum(views) AS total_views, sum(unique_users) AS total_users FROM ecommerce.pageviews_hourly GROUP BY day, country",
     ["analytics"], ["daily_country_stats"]),
    ("analytics", "mv_revenue_alerts",
     "CREATE MATERIALIZED VIEW analytics.mv_revenue_alerts TO analytics.revenue_alerts AS SELECT hour, category, revenue, 'high_revenue' AS alert_type FROM ecommerce.revenue_hourly WHERE revenue > 500",
     ["analytics"], ["revenue_alerts"]),
]


def demo_materialized_views():
    return pd.DataFrame(
        _MVS,
        columns=[
            "database", "name", "create_table_query",
            "dependencies_database", "dependencies_table",
        ],
    )


# ---------------------------------------------------------------------------
# demo_storage_metrics  ->  fetch_storage_metrics
# ---------------------------------------------------------------------------

_STORAGE = [
    ("ecommerce", "raw_events", 850_000, 34_000_000, 12_000_000, 68_000_000),
    ("ecommerce", "pageviews_hourly", 12_400, 620_000, 210_000, 1_240_000),
    ("ecommerce", "revenue_hourly", 8_200, 410_000, 140_000, 820_000),
    ("ecommerce", "cart_abandonment", 6_100, 305_000, 105_000, 610_000),
    ("ecommerce", "top_products", 200, 18_000, 6_000, 36_000),
    ("analytics", "user_sessions", 42_000, 3_200_000, 1_100_000, 6_400_000),
    ("analytics", "search_metrics", 3_800, 190_000, 65_000, 380_000),
    ("analytics", "refund_tracking", 1_200, 60_000, 20_000, 120_000),
    ("analytics", "daily_country_stats", 900, 45_000, 15_000, 90_000),
    ("analytics", "revenue_alerts", 340, 17_000, 6_000, 34_000),
]


def demo_storage_metrics():
    return pd.DataFrame(
        _STORAGE,
        columns=["database", "table", "rows", "bytes_on_disk", "compressed_bytes", "uncompressed_bytes"],
    )


# ---------------------------------------------------------------------------
# demo_partition_storage  ->  fetch_partition_storage
# ---------------------------------------------------------------------------


def demo_partition_storage():
    rows = []
    for db, tbl, total_rows, total_bytes, comp, _ in _STORAGE:
        # Simulate 2-3 partitions per table
        for i, frac in enumerate([0.6, 0.3, 0.1]):
            rows.append((db, tbl, f"20250{i + 1}", int(total_rows * frac),
                         int(total_bytes * frac), int(comp * frac)))
    return pd.DataFrame(
        rows,
        columns=["database", "table", "partition", "rows", "bytes_on_disk", "compressed_bytes"],
    )


# ---------------------------------------------------------------------------
# demo_mv_throughput  ->  fetch_mv_throughput
# ---------------------------------------------------------------------------


def demo_mv_throughput(hours=24):
    """Generate 5-minute interval throughput data for all 9 MVs over the last N hours."""
    import math

    mv_names = [
        "ecommerce.mv_pageviews_hourly",
        "ecommerce.mv_revenue_hourly",
        "ecommerce.mv_cart_abandonment",
        "ecommerce.mv_top_products",
        "analytics.mv_user_sessions",
        "analytics.mv_search_metrics",
        "analytics.mv_refund_tracking",
        "analytics.mv_daily_country_stats",
        "analytics.mv_revenue_alerts",
    ]
    base_rates = [500, 300, 250, 150, 400, 120, 60, 80, 40]

    rows = []
    intervals = int(hours * 12)  # 12 intervals per hour (5 min each)
    for mv_name, base in zip(mv_names, base_rates):
        for i in range(intervals):
            ts = _NOW - timedelta(minutes=5 * (intervals - i))
            # Sine wave variation
            variation = 1 + 0.3 * math.sin(2 * math.pi * i / 60)
            rows_written = int(base * variation)
            rows_read = int(rows_written * 1.2)
            bytes_written = rows_written * 120
            executions = max(1, rows_written // 50)
            avg_dur = 2.5 + 1.5 * math.sin(2 * math.pi * i / 40)

            rows.append([mv_name, ts, executions, rows_read, rows_written,
                         bytes_written, round(avg_dur, 2)])

    return pd.DataFrame(
        rows,
        columns=["view_name", "interval_start", "executions", "rows_read",
                 "rows_written", "bytes_written", "avg_duration_ms"],
    )


# ---------------------------------------------------------------------------
# demo_recent_throughput  ->  fetch_recent_throughput
# ---------------------------------------------------------------------------


def demo_recent_throughput():
    """Last 30 minutes of throughput data for lineage overlays."""
    mv_names = [
        "ecommerce.mv_pageviews_hourly",
        "ecommerce.mv_revenue_hourly",
        "ecommerce.mv_cart_abandonment",
        "ecommerce.mv_top_products",
        "analytics.mv_user_sessions",
        "analytics.mv_search_metrics",
        "analytics.mv_refund_tracking",
        "analytics.mv_daily_country_stats",
        "analytics.mv_revenue_alerts",
    ]
    base_rates = [500, 300, 250, 150, 400, 120, 60, 80, 40]

    rows = []
    for mv_name, base in zip(mv_names, base_rates):
        for i in range(6):  # 6 x 5-min buckets = 30 min
            ts = _NOW - timedelta(minutes=5 * (6 - i))
            rows.append([mv_name, ts, base + (i * 10)])

    return pd.DataFrame(rows, columns=["view_name", "interval_start", "rows_written"])


# ---------------------------------------------------------------------------
# demo_mv_errors  ->  fetch_mv_errors
# ---------------------------------------------------------------------------


def demo_mv_errors():
    """Return None (no errors) for a healthy demo."""
    return None


# ---------------------------------------------------------------------------
# demo_kafka_consumers  ->  fetch_kafka_consumers
# ---------------------------------------------------------------------------


def demo_kafka_consumers():
    """Return None (no Kafka tables) for the demo schema."""
    return None


# ---------------------------------------------------------------------------
# demo_create_table  ->  fetch_create_table
# ---------------------------------------------------------------------------

_CREATE_STMTS = {}
for db, name, query, _, _ in _MVS:
    _CREATE_STMTS[f"{db}.{name}"] = query

_CREATE_STMTS["ecommerce.raw_events"] = (
    "CREATE TABLE ecommerce.raw_events (\n"
    "    event_id UUID DEFAULT generateUUIDv4(),\n"
    "    event_time DateTime DEFAULT now(),\n"
    "    user_id UInt64,\n"
    "    session_id String,\n"
    "    event_type Enum8('page_view'=1, 'add_to_cart'=2, 'purchase'=3, 'refund'=4, 'search'=5),\n"
    "    product_id UInt32,\n"
    "    category String,\n"
    "    price Decimal64(2),\n"
    "    quantity UInt16 DEFAULT 1,\n"
    "    country LowCardinality(String),\n"
    "    device LowCardinality(String),\n"
    "    referrer LowCardinality(String)\n"
    ") ENGINE = MergeTree()\n"
    "ORDER BY (event_time, user_id)\n"
    "PARTITION BY toYYYYMM(event_time)"
)
_CREATE_STMTS["ecommerce.pageviews_hourly"] = (
    "CREATE TABLE ecommerce.pageviews_hourly (\n"
    "    hour DateTime, country LowCardinality(String),\n"
    "    device LowCardinality(String), views UInt64, unique_users UInt64\n"
    ") ENGINE = SummingMergeTree() ORDER BY (hour, country, device)"
)
_CREATE_STMTS["ecommerce.revenue_hourly"] = (
    "CREATE TABLE ecommerce.revenue_hourly (\n"
    "    hour DateTime, category String,\n"
    "    revenue Decimal64(2), orders UInt64, items_sold UInt64\n"
    ") ENGINE = SummingMergeTree() ORDER BY (hour, category)"
)
_CREATE_STMTS["ecommerce.cart_abandonment"] = (
    "CREATE TABLE ecommerce.cart_abandonment (\n"
    "    hour DateTime, category String,\n"
    "    carts_created UInt64, carts_purchased UInt64\n"
    ") ENGINE = SummingMergeTree() ORDER BY (hour, category)"
)
_CREATE_STMTS["ecommerce.top_products"] = (
    "CREATE TABLE ecommerce.top_products (\n"
    "    product_id UInt32, category String,\n"
    "    total_revenue Decimal64(2), total_orders UInt64, last_sold DateTime\n"
    ") ENGINE = ReplacingMergeTree(last_sold) ORDER BY (product_id)"
)
_CREATE_STMTS["analytics.user_sessions"] = (
    "CREATE TABLE analytics.user_sessions (\n"
    "    session_id String, user_id UInt64, session_start DateTime,\n"
    "    country LowCardinality(String), device LowCardinality(String),\n"
    "    referrer LowCardinality(String), page_views UInt32, events UInt32\n"
    ") ENGINE = ReplacingMergeTree(session_start) ORDER BY (session_id)"
)
_CREATE_STMTS["analytics.search_metrics"] = (
    "CREATE TABLE analytics.search_metrics (\n"
    "    hour DateTime, category String,\n"
    "    searches UInt64, unique_searchers UInt64\n"
    ") ENGINE = SummingMergeTree() ORDER BY (hour, category)"
)
_CREATE_STMTS["analytics.refund_tracking"] = (
    "CREATE TABLE analytics.refund_tracking (\n"
    "    hour DateTime, category String,\n"
    "    refunds UInt64, refund_amount Decimal64(2)\n"
    ") ENGINE = SummingMergeTree() ORDER BY (hour, category)"
)
_CREATE_STMTS["analytics.daily_country_stats"] = (
    "CREATE TABLE analytics.daily_country_stats (\n"
    "    day Date, country LowCardinality(String),\n"
    "    total_views UInt64, total_users UInt64\n"
    ") ENGINE = SummingMergeTree() ORDER BY (day, country)"
)
_CREATE_STMTS["analytics.revenue_alerts"] = (
    "CREATE TABLE analytics.revenue_alerts (\n"
    "    hour DateTime, category String,\n"
    "    revenue Decimal64(2), alert_type LowCardinality(String)\n"
    ") ENGINE = MergeTree() ORDER BY (hour, category)"
)


def demo_create_table(database, table):
    key = f"{database}.{table}"
    return _CREATE_STMTS.get(key, f"-- CREATE TABLE statement not available for {key}")
