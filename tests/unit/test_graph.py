"""Unit tests for chview.lineage.graph."""

import pandas as pd

from chview.lineage.graph import LineageEdge, TableNode, build_lineage


class TestTableNode:
    def test_full_name_auto_computed(self) -> None:
        node = TableNode("mydb", "mytable", "MergeTree")
        assert node.full_name == "mydb.mytable"

    def test_explicit_full_name_preserved(self) -> None:
        node = TableNode("mydb", "mytable", "MergeTree", full_name="custom.name")
        assert node.full_name == "custom.name"

    def test_engine_stored(self) -> None:
        node = TableNode("db", "t", "SummingMergeTree")
        assert node.engine == "SummingMergeTree"


class TestLineageEdge:
    def test_fields(self) -> None:
        edge = LineageEdge("db.A", "db.B", "db.MV")
        assert edge.source == "db.A"
        assert edge.target == "db.B"
        assert edge.mv_name == "db.MV"


class TestBuildLineage:
    def test_mv_added_as_node(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        assert "mydb.mv_orders" in lineage.nodes
        assert lineage.nodes["mydb.mv_orders"].engine == "MaterializedView"

    def test_mv_in_mv_names(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        assert "mydb.mv_orders" in lineage.mv_names

    def test_source_table_added(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        assert "mydb.orders" in lineage.nodes

    def test_target_table_added(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        assert "mydb.orders_agg" in lineage.nodes

    def test_target_in_target_names(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        assert "mydb.orders_agg" in lineage.target_names

    def test_edges_source_to_mv(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        edge_sources = {(e.source, e.target) for e in lineage.edges}
        assert ("mydb.orders", "mydb.mv_orders") in edge_sources

    def test_edges_mv_to_target(self, simple_mv_df: pd.DataFrame) -> None:
        lineage = build_lineage(simple_mv_df)
        edge_sources = {(e.source, e.target) for e in lineage.edges}
        assert ("mydb.mv_orders", "mydb.orders_agg") in edge_sources

    def test_engine_lookup_from_schema(
        self, simple_mv_df: pd.DataFrame, schema_df: pd.DataFrame
    ) -> None:
        lineage = build_lineage(simple_mv_df, schema_df)
        # The target table should get the engine from schema
        assert lineage.nodes["mydb.orders_agg"].engine == "SummingMergeTree"

    def test_chained_mvs(
        self, multi_mv_df: pd.DataFrame, schema_df: pd.DataFrame
    ) -> None:
        lineage = build_lineage(multi_mv_df, schema_df)
        # Both MVs present
        assert "db.mv_a" in lineage.nodes
        assert "db.mv_b" in lineage.nodes
        # Source and both targets present
        assert "db.raw" in lineage.nodes
        assert "db.agg_a" in lineage.nodes
        assert "db.agg_b" in lineage.nodes

    def test_empty_df_returns_empty_lineage(self) -> None:
        empty_df = pd.DataFrame(
            columns=[
                "database",
                "name",
                "create_table_query",
                "dependencies_database",
                "dependencies_table",
            ]
        )
        lineage = build_lineage(empty_df)
        assert lineage.nodes == {}
        assert lineage.edges == []

    def test_dependencies_merged(self) -> None:
        """Explicit ClickHouse dependency metadata is included as source edges."""
        df = pd.DataFrame(
            [
                {
                    "database": "db",
                    "name": "mv",
                    "create_table_query": (
                        "CREATE MATERIALIZED VIEW db.mv TO db.out AS SELECT id FROM db.src"
                    ),
                    "dependencies_database": ["db"],
                    "dependencies_table": ["extra"],
                }
            ]
        )
        lineage = build_lineage(df)
        node_ids = set(lineage.nodes.keys())
        assert "db.extra" in node_ids
