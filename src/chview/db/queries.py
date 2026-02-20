"""SQL query builder with DRY pattern for database filtering."""

from typing import Optional


def build_database_filter(
    column: str = "database",
    database: Optional[str] = None,
    exclude_system: bool = True,
) -> tuple[str, dict]:
    """Build WHERE clause with optional database filter.

    Args:
        column: Database column name
        database: Optional database name to filter by
        exclude_system: Whether to exclude system databases

    Returns:
        Tuple of (where_clause, parameters)
    """
    conditions = []
    params = {}

    if database and database != "All":
        conditions.append(f"{column} = %(database)s")
        params["database"] = database
    elif exclude_system:
        conditions.append(
            f"{column} NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
        )

    if conditions:
        return "WHERE " + " AND ".join(conditions), params
    return "", {}


def build_system_tables_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching table schema information."""
    where_clause, params = build_database_filter(
        "database", database, exclude_system=True
    )

    query = f"""
        SELECT
            database,
            name,
            engine,
            total_rows,
            total_bytes
        FROM system.tables
        {where_clause}
        ORDER BY database, name
    """
    return query, params


def build_materialized_views_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching materialized views."""
    where_clause, params = build_database_filter(
        "database", database, exclude_system=False
    )

    if where_clause:
        where_clause += " AND engine = 'MaterializedView'"
    else:
        where_clause = "WHERE engine = 'MaterializedView'"

    query = f"""
        SELECT
            database,
            name,
            create_table_query,
            dependencies_database,
            dependencies_table
        FROM system.tables
        {where_clause}
        ORDER BY database, name
    """
    return query, params


def build_storage_metrics_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching storage metrics."""
    where_clause, params = build_database_filter(
        "database", database, exclude_system=True
    )

    if where_clause:
        where_clause += " AND active = 1"
    else:
        where_clause = "WHERE active = 1"

    query = f"""
        SELECT
            database,
            table,
            sum(rows) AS rows,
            sum(bytes_on_disk) AS bytes_on_disk,
            sum(data_compressed_bytes) AS compressed_bytes,
            sum(data_uncompressed_bytes) AS uncompressed_bytes
        FROM system.parts
        {where_clause}
        GROUP BY database, table
        ORDER BY database, table
        SETTINGS max_execution_time = 120
    """
    return query, params


def build_partition_storage_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching partition-level storage metrics."""
    where_clause, params = build_database_filter(
        "database", database, exclude_system=True
    )

    if where_clause:
        where_clause += " AND active = 1"
    else:
        where_clause = "WHERE active = 1"

    query = f"""
        SELECT
            database,
            table,
            partition,
            sum(rows) AS rows,
            sum(bytes_on_disk) AS bytes_on_disk,
            sum(data_compressed_bytes) AS compressed_bytes
        FROM system.parts
        {where_clause}
        GROUP BY database, table, partition
        ORDER BY bytes_on_disk DESC
        LIMIT 5000
        SETTINGS max_execution_time = 120
    """
    return query, params


def build_cluster_info_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching cluster overview information."""
    params = {}

    if database and database != "All":
        query = """
            SELECT
                version(),
                uptime(),
                1,
                (SELECT count() FROM system.tables WHERE database = %(database)s),
                (SELECT count() FROM system.tables
                 WHERE database = %(database)s AND engine = 'MaterializedView'),
                (SELECT sum(bytes_on_disk) FROM system.parts
                 WHERE active = 1 AND database = %(database)s)
            SETTINGS max_execution_time = 120
        """
        params["database"] = database
    else:
        query = """
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
        """

    return query, params


def build_mv_throughput_query(
    hours: int = 24, database: Optional[str] = None
) -> tuple[str, dict]:
    """Build query for fetching MV throughput metrics."""
    params = {"hours": hours}

    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    query = f"""
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
    """
    return query, params


def build_recent_throughput_query(
    minutes: int = 30, database: Optional[str] = None
) -> tuple[str, dict]:
    """Build query for fetching recent throughput data."""
    params = {"minutes": minutes}

    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    query = f"""
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
    """
    return query, params


def build_mv_errors_query(
    hours: int = 24, database: Optional[str] = None
) -> tuple[str, dict]:
    """Build query for fetching MV errors."""
    params = {"hours": hours}

    if database and database != "All":
        db_filter = "AND view_name LIKE %(db_pattern)s"
        params["db_pattern"] = f"{database}.%"
    else:
        db_filter = ""

    query = f"""
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
    """
    return query, params


def build_kafka_consumers_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query for fetching Kafka consumer health data."""
    params = {}

    if database and database != "All":
        db_filter = "AND database = %(database)s"
        params["database"] = database
    else:
        db_filter = ""

    query = f"""
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
        WHERE 1=1 {db_filter}
        ORDER BY database, table, consumer_id
        LIMIT 1000
    """
    return query, params


def build_kafka_check_query(database: Optional[str] = None) -> tuple[str, dict]:
    """Build query to check if any Kafka tables exist."""
    params = {}

    if database and database != "All":
        query = """
            SELECT count()
            FROM system.tables
            WHERE engine = 'Kafka'
              AND database = %(database)s
        """
        params["database"] = database
    else:
        query = """
            SELECT count()
            FROM system.tables
            WHERE engine = 'Kafka'
        """

    return query, params
