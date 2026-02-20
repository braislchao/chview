"""Unit tests for chview.db.queries."""


from chview.db.queries import (
    build_database_filter,
    build_kafka_check_query,
    build_kafka_consumers_query,
    build_materialized_views_query,
    build_mv_errors_query,
    build_mv_throughput_query,
    build_partition_storage_query,
    build_recent_throughput_query,
    build_storage_metrics_query,
    build_system_tables_query,
)


class TestBuildDatabaseFilter:
    def test_no_filter_returns_empty(self) -> None:
        clause, params = build_database_filter(exclude_system=False)
        assert clause == ""
        assert params == {}

    def test_exclude_system_default(self) -> None:
        clause, params = build_database_filter()
        assert "WHERE" in clause
        assert "system" in clause
        assert params == {}

    def test_specific_database(self) -> None:
        clause, params = build_database_filter(database="mydb")
        assert "WHERE" in clause
        assert "%(database)s" in clause
        assert params == {"database": "mydb"}

    def test_all_database_treated_as_no_filter_with_exclude_system(self) -> None:
        clause, params = build_database_filter(database="All")
        # "All" is treated as no specific filter → falls through to exclude_system
        assert "system" in clause
        assert "database" not in params

    def test_custom_column_name(self) -> None:
        clause, params = build_database_filter(column="db", database="foo")
        assert "db = %(database)s" in clause
        assert params == {"database": "foo"}

    def test_no_filter_no_exclude(self) -> None:
        clause, params = build_database_filter(database=None, exclude_system=False)
        assert clause == ""
        assert params == {}


class TestBuildSystemTablesQuery:
    def test_returns_query_and_params(self) -> None:
        query, params = build_system_tables_query()
        assert "system.tables" in query
        assert "database" in query

    def test_with_database_filter(self) -> None:
        query, params = build_system_tables_query(database="mydb")
        assert "%(database)s" in query
        assert params == {"database": "mydb"}

    def test_without_filter_excludes_system(self) -> None:
        query, params = build_system_tables_query()
        assert "system" in query  # the NOT IN system clause
        assert params == {}


class TestBuildMaterializedViewsQuery:
    def test_always_includes_materialized_view_filter(self) -> None:
        query, _ = build_materialized_views_query()
        assert "MaterializedView" in query

    def test_with_database(self) -> None:
        query, params = build_materialized_views_query(database="db1")
        assert "%(database)s" in query
        assert params == {"database": "db1"}

    def test_without_database(self) -> None:
        query, params = build_materialized_views_query()
        assert "MaterializedView" in query
        assert params == {}


class TestBuildStorageMetricsQuery:
    def test_active_filter_always_present(self) -> None:
        query, _ = build_storage_metrics_query()
        assert "active = 1" in query

    def test_with_database(self) -> None:
        query, params = build_storage_metrics_query(database="analytics")
        assert "%(database)s" in query
        assert params["database"] == "analytics"

    def test_selects_correct_columns(self) -> None:
        query, _ = build_storage_metrics_query()
        assert "bytes_on_disk" in query
        assert "compressed_bytes" in query


class TestBuildPartitionStorageQuery:
    def test_has_partition_column(self) -> None:
        query, _ = build_partition_storage_query()
        assert "partition" in query

    def test_has_limit(self) -> None:
        query, _ = build_partition_storage_query()
        assert "LIMIT" in query

    def test_with_database(self) -> None:
        _, params = build_partition_storage_query(database="db2")
        assert params["database"] == "db2"


class TestBuildMvThroughputQuery:
    def test_default_hours_in_params(self) -> None:
        _, params = build_mv_throughput_query()
        assert params["hours"] == 24

    def test_custom_hours(self) -> None:
        _, params = build_mv_throughput_query(hours=6)
        assert params["hours"] == 6

    def test_database_pattern_filter(self) -> None:
        query, params = build_mv_throughput_query(database="mydb")
        assert "db_pattern" in params
        assert params["db_pattern"] == "mydb.%"
        assert "LIKE" in query

    def test_no_database_no_pattern(self) -> None:
        query, params = build_mv_throughput_query()
        assert "db_pattern" not in params


class TestBuildRecentThroughputQuery:
    def test_default_minutes_in_params(self) -> None:
        _, params = build_recent_throughput_query()
        assert params["minutes"] == 30

    def test_custom_minutes(self) -> None:
        _, params = build_recent_throughput_query(minutes=5)
        assert params["minutes"] == 5

    def test_database_pattern(self) -> None:
        _, params = build_recent_throughput_query(database="db")
        assert params["db_pattern"] == "db.%"


class TestBuildMvErrorsQuery:
    def test_includes_exception_status(self) -> None:
        query, _ = build_mv_errors_query()
        assert "ExceptionBeforeStart" in query or "Exception" in query

    def test_default_hours(self) -> None:
        _, params = build_mv_errors_query()
        assert params["hours"] == 24

    def test_with_database(self) -> None:
        _, params = build_mv_errors_query(database="prod")
        assert params["db_pattern"] == "prod.%"


class TestBuildKafkaConsumersQuery:
    def test_array_join_present(self) -> None:
        query, _ = build_kafka_consumers_query()
        assert "ARRAY JOIN" in query

    def test_with_database(self) -> None:
        query, params = build_kafka_consumers_query(database="kafka_db")
        assert params["database"] == "kafka_db"

    def test_without_database(self) -> None:
        _, params = build_kafka_consumers_query()
        assert params == {}


class TestBuildKafkaCheckQuery:
    def test_counts_kafka_engine(self) -> None:
        query, _ = build_kafka_check_query()
        assert "Kafka" in query
        assert "count()" in query

    def test_with_database(self) -> None:
        query, params = build_kafka_check_query(database="events")
        assert "%(database)s" in query
        assert params["database"] == "events"

    def test_without_database(self) -> None:
        query, params = build_kafka_check_query()
        assert params == {}
