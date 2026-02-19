"""Shared pytest fixtures for CHView test suite."""

import pandas as pd
import pytest

from chview.lineage.graph import LineageEdge, LineageGraph, TableNode


@pytest.fixture
def simple_mv_df() -> pd.DataFrame:
    """Minimal MV DataFrame: one MV with a TO clause."""
    return pd.DataFrame(
        [
            {
                "database": "mydb",
                "name": "mv_orders",
                "create_table_query": (
                    "CREATE MATERIALIZED VIEW mydb.mv_orders TO mydb.orders_agg "
                    "AS SELECT toDate(created_at) AS day, count() AS cnt "
                    "FROM mydb.orders GROUP BY day"
                ),
                "dependencies_database": [],
                "dependencies_table": [],
            }
        ]
    )


@pytest.fixture
def multi_mv_df() -> pd.DataFrame:
    """Two chained MVs: raw → mv_a → mv_b."""
    return pd.DataFrame(
        [
            {
                "database": "db",
                "name": "mv_a",
                "create_table_query": (
                    "CREATE MATERIALIZED VIEW db.mv_a TO db.agg_a "
                    "AS SELECT id FROM db.raw"
                ),
                "dependencies_database": [],
                "dependencies_table": [],
            },
            {
                "database": "db",
                "name": "mv_b",
                "create_table_query": (
                    "CREATE MATERIALIZED VIEW db.mv_b TO db.agg_b "
                    "AS SELECT id FROM db.agg_a"
                ),
                "dependencies_database": [],
                "dependencies_table": [],
            },
        ]
    )


@pytest.fixture
def schema_df() -> pd.DataFrame:
    """Small schema DataFrame covering tables used in fixtures above."""
    return pd.DataFrame(
        [
            {"database": "mydb", "name": "orders", "engine": "MergeTree"},
            {"database": "mydb", "name": "orders_agg", "engine": "SummingMergeTree"},
            {"database": "db", "name": "raw", "engine": "MergeTree"},
            {"database": "db", "name": "agg_a", "engine": "SummingMergeTree"},
            {"database": "db", "name": "agg_b", "engine": "AggregatingMergeTree"},
        ]
    )


@pytest.fixture
def linear_lineage() -> LineageGraph:
    """A -> MV -> B linear graph (3 nodes, 2 edges)."""
    nodes = {
        "db.A": TableNode("db", "A", "MergeTree"),
        "db.MV": TableNode("db", "MV", "MaterializedView"),
        "db.B": TableNode("db", "B", "SummingMergeTree"),
    }
    edges = [
        LineageEdge("db.A", "db.MV", "db.MV"),
        LineageEdge("db.MV", "db.B", "db.MV"),
    ]
    graph = LineageGraph(nodes=nodes, edges=edges)
    graph.mv_names.add("db.MV")
    graph.target_names.add("db.B")
    return graph


@pytest.fixture
def diamond_lineage() -> LineageGraph:
    """
    Diamond pattern:
        A ─► MV1 ─► C
        B ─► MV2 ─► C
    """
    nodes = {
        "db.A": TableNode("db", "A", "MergeTree"),
        "db.B": TableNode("db", "B", "MergeTree"),
        "db.MV1": TableNode("db", "MV1", "MaterializedView"),
        "db.MV2": TableNode("db", "MV2", "MaterializedView"),
        "db.C": TableNode("db", "C", "SummingMergeTree"),
    }
    edges = [
        LineageEdge("db.A", "db.MV1", "db.MV1"),
        LineageEdge("db.MV1", "db.C", "db.MV1"),
        LineageEdge("db.B", "db.MV2", "db.MV2"),
        LineageEdge("db.MV2", "db.C", "db.MV2"),
    ]
    graph = LineageGraph(nodes=nodes, edges=edges)
    graph.mv_names.update({"db.MV1", "db.MV2"})
    graph.target_names.add("db.C")
    return graph
