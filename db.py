import os

import clickhouse_connect
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

_CLIENT_KWARGS = None


def _client_kwargs():
    """Parse connection env vars once, return cached kwargs dict."""
    global _CLIENT_KWARGS
    if _CLIENT_KWARGS is None:
        _CLIENT_KWARGS = dict(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
            secure=os.getenv("CLICKHOUSE_SECURE", "True").lower() in ("true", "1", "yes"),
            connect_timeout=10,
            send_receive_timeout=120,
        )
    return _CLIENT_KWARGS


def get_client():
    """Create a new ClickHouse client per call to avoid session locking."""
    return clickhouse_connect.get_client(**_client_kwargs())


def test_connection():
    """Test the ClickHouse connection. Returns (success, version, host)."""
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = os.getenv("CLICKHOUSE_PORT", "8443")
    try:
        client = get_client()
        result = client.query("SELECT version()")
        version = result.first_row[0]
        return True, version, f"{host}:{port}"
    except Exception as e:
        return False, str(e), f"{host}:{port}"


def fetch_databases():
    """Fetch list of all user databases."""
    client = get_client()
    result = client.query("""
        SELECT name
        FROM system.databases
        WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
        ORDER BY name
    """)
    return [row[0] for row in result.result_rows]


def fetch_schema(database=None):
    """Fetch all non-system tables. Returns a DataFrame.
    
    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()
    
    if database and database != "All":
        query = """
            SELECT
                database,
                name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE database = %(database)s
            ORDER BY database, name
        """
        result = client.query(query, parameters={"database": database})
    else:
        query = """
            SELECT
                database,
                name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
            ORDER BY database, name
        """
        result = client.query(query)

    return pd.DataFrame(
        result.result_rows,
        columns=["database", "name", "engine", "total_rows", "total_bytes"],
    )


def fetch_materialized_views(database=None):
    """Fetch all materialized views with their create queries and dependencies.
    
    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()
    
    if database and database != "All":
        query = """
            SELECT
                database,
                name,
                create_table_query,
                dependencies_database,
                dependencies_table
            FROM system.tables
            WHERE engine = 'MaterializedView'
              AND database = %(database)s
            ORDER BY database, name
        """
        result = client.query(query, parameters={"database": database})
    else:
        query = """
            SELECT
                database,
                name,
                create_table_query,
                dependencies_database,
                dependencies_table
            FROM system.tables
            WHERE engine = 'MaterializedView'
            ORDER BY database, name
        """
        result = client.query(query)
    return pd.DataFrame(
        result.result_rows,
        columns=[
            "database",
            "name",
            "create_table_query",
            "dependencies_database",
            "dependencies_table",
        ],
    )


def fetch_storage_metrics(database=None):
    """Fetch storage metrics from active parts, grouped by database/table.
    
    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()
    
    if database and database != "All":
        query = """
            SELECT
                database,
                table,
                sum(rows) AS rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_compressed_bytes) AS compressed_bytes,
                sum(data_uncompressed_bytes) AS uncompressed_bytes
            FROM system.parts
            WHERE active = 1
              AND database = %(database)s
            GROUP BY database, table
            ORDER BY database, table
            SETTINGS max_execution_time = 120
        """
        result = client.query(query, parameters={"database": database})
    else:
        query = """
            SELECT
                database,
                table,
                sum(rows) AS rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_compressed_bytes) AS compressed_bytes,
                sum(data_uncompressed_bytes) AS uncompressed_bytes
            FROM system.parts
            WHERE active = 1
            GROUP BY database, table
            ORDER BY database, table
            SETTINGS max_execution_time = 120
        """
        result = client.query(query)
    return pd.DataFrame(
        result.result_rows,
        columns=[
            "database",
            "table",
            "rows",
            "bytes_on_disk",
            "compressed_bytes",
            "uncompressed_bytes",
        ],
    )


def fetch_partition_storage(database=None):
    """Fetch partition-level storage metrics for treemap visualization.

    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()

    if database and database != "All":
        query = """
            SELECT
                database,
                table,
                partition,
                sum(rows) AS rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_compressed_bytes) AS compressed_bytes
            FROM system.parts
            WHERE active = 1
              AND database = %(database)s
            GROUP BY database, table, partition
            ORDER BY bytes_on_disk DESC
            LIMIT 5000
            SETTINGS max_execution_time = 120
        """
        result = client.query(query, parameters={"database": database})
    else:
        query = """
            SELECT
                database,
                table,
                partition,
                sum(rows) AS rows,
                sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_compressed_bytes) AS compressed_bytes
            FROM system.parts
            WHERE active = 1
            GROUP BY database, table, partition
            ORDER BY bytes_on_disk DESC
            LIMIT 5000
            SETTINGS max_execution_time = 120
        """
        result = client.query(query)
    return pd.DataFrame(
        result.result_rows,
        columns=[
            "database",
            "table",
            "partition",
            "rows",
            "bytes_on_disk",
            "compressed_bytes",
        ],
    )


def fetch_cluster_info(database=None):
    """Fetch cluster overview info. Returns a dict with version, uptime, counts, disk.

    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()

    if database and database != "All":
        result = client.query("""
            SELECT
                version(),
                uptime(),
                1,
                (SELECT count() FROM system.tables WHERE database = %(database)s),
                (SELECT count() FROM system.tables WHERE database = %(database)s AND engine = 'MaterializedView'),
                (SELECT sum(bytes_on_disk) FROM system.parts WHERE active = 1 AND database = %(database)s)
            SETTINGS max_execution_time = 120
        """, parameters={"database": database})
    else:
        result = client.query("""
            SELECT
                version(),
                uptime(),
                (SELECT count() FROM system.databases
                 WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')),
                (SELECT count() FROM system.tables
                 WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')),
                (SELECT count() FROM system.tables WHERE engine = 'MaterializedView'),
                (SELECT sum(bytes_on_disk) FROM system.parts WHERE active = 1)
            SETTINGS max_execution_time = 120
        """)

    row = result.first_row
    return {
        "version": row[0],
        "uptime_seconds": row[1],
        "user_databases": row[2],
        "user_tables": row[3],
        "mv_count": row[4],
        "total_disk_bytes": row[5],
    }


def fetch_mv_throughput(hours=24, database=None):
    """Fetch MV throughput metrics in 5-minute intervals.

    Tries query_views_log first, falls back to query_log.

    Args:
        hours: Number of hours to look back.
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()

    params = {"hours": hours}
    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    # Primary: query_views_log (available in newer ClickHouse versions)
    try:
        result = client.query(
            f"""
            SELECT
                view_name,
                toStartOfFiveMinutes(event_time) AS interval_start,
                count() AS executions,
                sum(read_rows) AS rows_read,
                sum(written_rows) AS rows_written,
                sum(written_bytes) AS bytes_written,
                avg(view_duration_ms) AS avg_duration_ms
            FROM system.query_views_log
            WHERE event_time >= now() - INTERVAL %(hours)s HOUR
              AND status = 'QueryFinish'
              {db_filter}
            GROUP BY view_name, interval_start
            ORDER BY view_name, interval_start
            """,
            parameters=params,
        )
        return pd.DataFrame(
            result.result_rows,
            columns=[
                "view_name",
                "interval_start",
                "executions",
                "rows_read",
                "rows_written",
                "bytes_written",
                "avg_duration_ms",
            ],
        )
    except Exception:
        pass

    # Fallback: query_log for older versions
    try:
        result = client.query(
            f"""
            SELECT
                tables[1] AS view_name,
                toStartOfFiveMinutes(event_time) AS interval_start,
                count() AS executions,
                sum(read_rows) AS rows_read,
                sum(written_rows) AS rows_written,
                sum(written_bytes) AS bytes_written,
                avg(query_duration_ms) AS avg_duration_ms
            FROM system.query_log
            WHERE event_time >= now() - INTERVAL %(hours)s HOUR
              AND type = 'QueryFinish'
              AND has(tables, (SELECT concat(database, '.', name)
                               FROM system.tables
                               WHERE engine = 'MaterializedView' LIMIT 1))
              {db_filter}
            GROUP BY view_name, interval_start
            ORDER BY view_name, interval_start
            """,
            parameters=params,
        )
        return pd.DataFrame(
            result.result_rows,
            columns=[
                "view_name",
                "interval_start",
                "executions",
                "rows_read",
                "rows_written",
                "bytes_written",
                "avg_duration_ms",
            ],
        )
    except Exception:
        return pd.DataFrame(
            columns=[
                "view_name",
                "interval_start",
                "executions",
                "rows_read",
                "rows_written",
                "bytes_written",
                "avg_duration_ms",
            ]
        )


def fetch_recent_throughput(minutes=30, database=None):
    """Fetch last N minutes of MV throughput in 5-minute buckets for lineage overlays.

    Args:
        minutes: Number of minutes to look back.
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()

    params = {"minutes": minutes}
    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    try:
        result = client.query(
            f"""
            SELECT
                view_name,
                toStartOfFiveMinutes(event_time) AS interval_start,
                sum(written_rows) AS rows_written
            FROM system.query_views_log
            WHERE event_time >= now() - INTERVAL %(minutes)s MINUTE
              AND status = 'QueryFinish'
              {db_filter}
            GROUP BY view_name, interval_start
            ORDER BY view_name, interval_start
            """,
            parameters=params,
        )
        return pd.DataFrame(
            result.result_rows,
            columns=["view_name", "interval_start", "rows_written"],
        )
    except Exception:
        return pd.DataFrame(columns=["view_name", "interval_start", "rows_written"])


def fetch_create_table(database, table):
    """Fetch the CREATE TABLE statement for a given table."""
    client = get_client()
    try:
        result = client.query(
            "SHOW CREATE TABLE %(db)s.%(tbl)s",
            parameters={"db": database, "tbl": table},
        )
        return result.first_row[0]
    except Exception:
        # Fallback: use backtick quoting via format string
        try:
            result = client.query(f"SHOW CREATE TABLE `{database}`.`{table}`")
            return result.first_row[0]
        except Exception as e:
            return f"-- Error fetching CREATE TABLE: {e}"


def fetch_create_view(database, view):
    """Fetch the CREATE VIEW or CREATE MATERIALIZED VIEW statement for a given view."""
    client = get_client()
    try:
        result = client.query(
            "SHOW CREATE VIEW %(db)s.%(view)s",
            parameters={"db": database, "view": view},
        )
        return result.first_row[0]
    except Exception:
        # Fallback: use backtick quoting via format string
        try:
            result = client.query(f"SHOW CREATE VIEW `{database}`.`{view}`")
            return result.first_row[0]
        except Exception:
            # Try as materialized view
            try:
                result = client.query(f"SHOW CREATE TABLE `{database}`.`{view}`")
                return result.first_row[0]
            except Exception as e:
                return f"-- Error fetching CREATE VIEW: {e}"


def fetch_mv_errors(hours=24, database=None):
    """Fetch MV errors from query_views_log. Returns DataFrame or None.

    Args:
        hours: Number of hours to look back.
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()

    params = {"hours": hours}
    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    try:
        result = client.query(
            f"""
            SELECT
                view_name,
                exception_code,
                exception,
                event_time,
                count() OVER (PARTITION BY view_name) AS error_count
            FROM system.query_views_log
            WHERE event_time >= now() - INTERVAL %(hours)s HOUR
              AND status IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
              {db_filter}
            ORDER BY event_time DESC
            LIMIT 100
            """,
            parameters=params,
        )
        df = pd.DataFrame(
            result.result_rows,
            columns=[
                "view_name",
                "exception_code",
                "exception",
                "event_time",
                "error_count",
            ],
        )
        return df if not df.empty else None
    except Exception:
        return None


def fetch_kafka_consumers(database=None):
    """Fetch Kafka consumer health data. Returns DataFrame or None if no Kafka tables.

    Args:
        database: Optional database name to filter by. If None, returns all databases.
    """
    client = get_client()
    try:
        # First check if any Kafka engine tables exist
        if database and database != "All":
            check = client.query("""
                SELECT count()
                FROM system.tables
                WHERE engine = 'Kafka'
                  AND database = %(database)s
            """, parameters={"database": database})
        else:
            check = client.query("""
                SELECT count()
                FROM system.tables
                WHERE engine = 'Kafka'
            """)
        if check.first_row[0] == 0:
            return None

        if database and database != "All":
            db_filter_query = "AND database = %(database)s"
            params = {"database": database}
        else:
            db_filter_query = ""
            params = {}

        result = client.query(f"""
            SELECT
                database,
                table,
                consumer_id,
                assignments.topic AS topic,
                assignments.partition_id AS partition_id,
                assignments.current_offset AS current_offset,
                last_poll_time,
                num_messages_read,
                num_rebalance_revocations + num_rebalance_assignments AS rebalance_count,
                is_currently_used,
                dateDiff('second', last_poll_time, now()) AS seconds_since_poll
            FROM system.kafka_consumers
            ARRAY JOIN assignments
            WHERE 1=1 {db_filter_query}
            ORDER BY database, table, consumer_id
            LIMIT 1000
        """, parameters=params)
        df = pd.DataFrame(
            result.result_rows,
            columns=[
                "database",
                "table",
                "consumer_id",
                "topic",
                "partition_id",
                "current_offset",
                "last_poll_time",
                "num_messages_read",
                "rebalance_count",
                "is_currently_used",
                "seconds_since_poll",
            ],
        )
        return df if not df.empty else None
    except Exception:
        return None
