"""High-level data repository for ClickHouse operations."""

from typing import Optional

import pandas as pd

from chview.db import queries
from chview.db.client import ClickHouseClient


class ClickHouseRepository:
    """Repository for ClickHouse data access."""

    def __init__(self) -> None:
        self._client = ClickHouseClient()

    def fetch_databases(self) -> list[str]:
        """Fetch list of all user databases."""
        client = self._client.get_client()
        result = client.query(
            """
            SELECT name
            FROM system.databases
            WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
            ORDER BY name
        """
        )
        return [row[0] for row in result.result_rows]

    def fetch_schema(self, database: Optional[str] = None) -> pd.DataFrame:
        """Fetch all non-system tables."""
        query, params = queries.build_system_tables_query(database)
        client = self._client.get_client()
        result = client.query(query, parameters=params)

        return pd.DataFrame(
            result.result_rows,
            columns=["database", "name", "engine", "total_rows", "total_bytes"],
        )

    def fetch_materialized_views(self, database: Optional[str] = None) -> pd.DataFrame:
        """Fetch all materialized views with their create queries and dependencies."""
        query, params = queries.build_materialized_views_query(database)
        client = self._client.get_client()
        result = client.query(query, parameters=params)

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

    def fetch_storage_metrics(self, database: Optional[str] = None) -> pd.DataFrame:
        """Fetch storage metrics from active parts, grouped by database/table."""
        query, params = queries.build_storage_metrics_query(database)
        client = self._client.get_client()
        result = client.query(query, parameters=params)

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

    def fetch_partition_storage(self, database: Optional[str] = None) -> pd.DataFrame:
        """Fetch partition-level storage metrics for treemap visualization."""
        query, params = queries.build_partition_storage_query(database)
        client = self._client.get_client()
        result = client.query(query, parameters=params)

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

    def fetch_cluster_info(self, database: Optional[str] = None) -> dict:
        """Fetch cluster overview info."""
        query, params = queries.build_cluster_info_query(database)
        client = self._client.get_client()
        result = client.query(query, parameters=params)

        row = result.first_row
        return {
            "version": row[0],
            "uptime_seconds": row[1],
            "user_databases": row[2],
            "user_tables": row[3],
            "mv_count": row[4],
            "total_disk_bytes": row[5],
        }

    def fetch_mv_throughput(
        self, hours: int = 24, database: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch MV throughput metrics in 5-minute intervals."""
        # Try query_views_log first (newer ClickHouse versions)
        try:
            query, params = queries.build_mv_throughput_query(hours, database)
            client = self._client.get_client()
            result = client.query(query, parameters=params)

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

    def fetch_recent_throughput(
        self, minutes: int = 30, database: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch last N minutes of MV throughput."""
        try:
            query, params = queries.build_recent_throughput_query(minutes, database)
            client = self._client.get_client()
            result = client.query(query, parameters=params)

            return pd.DataFrame(
                result.result_rows,
                columns=["view_name", "interval_start", "rows_written"],
            )
        except Exception:
            return pd.DataFrame(columns=["view_name", "interval_start", "rows_written"])

    def fetch_create_table(self, database: str, table: str) -> str:
        """Fetch the CREATE TABLE statement for a given table."""
        client = self._client.get_client()
        try:
            result = client.query(
                "SHOW CREATE TABLE %(db)s.%(tbl)s",
                parameters={"db": database, "tbl": table},
            )
            return result.first_row[0]
        except Exception:
            # Fallback: use backtick quoting
            try:
                result = client.query(f"SHOW CREATE TABLE `{database}`.`{table}`")
                return result.first_row[0]
            except Exception as e:
                return f"-- Error fetching CREATE TABLE: {e}"

    def fetch_create_view(self, database: str, view: str) -> str:
        """Fetch the CREATE VIEW or CREATE MATERIALIZED VIEW statement."""
        client = self._client.get_client()
        try:
            result = client.query(
                "SHOW CREATE VIEW %(db)s.%(view)s",
                parameters={"db": database, "view": view},
            )
            return result.first_row[0]
        except Exception:
            # Try as materialized view
            try:
                result = client.query(f"SHOW CREATE TABLE `{database}`.`{view}`")
                return result.first_row[0]
            except Exception as e:
                return f"-- Error fetching CREATE VIEW: {e}"

    def fetch_mv_errors(
        self, hours: int = 24, database: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """Fetch MV errors from query_views_log."""
        try:
            query, params = queries.build_mv_errors_query(hours, database)
            client = self._client.get_client()
            result = client.query(query, parameters=params)

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

    def fetch_kafka_consumers(
        self, database: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """Fetch Kafka consumer health data."""
        try:
            # First check if any Kafka tables exist
            check_query, check_params = queries.build_kafka_check_query(database)
            client = self._client.get_client()
            check = client.query(check_query, parameters=check_params)

            if check.first_row[0] == 0:
                return None

            # Fetch consumer data
            query, params = queries.build_kafka_consumers_query(database)
            result = client.query(query, parameters=params)

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
