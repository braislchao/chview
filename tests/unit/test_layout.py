"""Unit tests for chview.lineage.layout."""

import pytest

from chview.lineage.graph import LineageEdge, LineageGraph, TableNode
from chview.lineage.layout import calculate_positions, get_connected_subgraph


class TestCalculatePositions:
    def test_all_nodes_get_positions(self, linear_lineage: LineageGraph) -> None:
        positions = calculate_positions(linear_lineage)
        assert set(positions.keys()) == set(linear_lineage.nodes.keys())

    def test_positions_are_tuples_of_floats(self, linear_lineage: LineageGraph) -> None:
        positions = calculate_positions(linear_lineage)
        for pos in positions.values():
            assert len(pos) == 2
            assert isinstance(pos[0], float)
            assert isinstance(pos[1], float)

    def test_source_is_level_zero(self, linear_lineage: LineageGraph) -> None:
        """db.A has no incoming edges → level 0 → lowest x."""
        positions = calculate_positions(linear_lineage)
        x_a = positions["db.A"][0]
        x_mv = positions["db.MV"][0]
        x_b = positions["db.B"][0]
        # Levels should be strictly increasing left-to-right
        assert x_a < x_mv < x_b

    def test_empty_graph(self) -> None:
        lineage = LineageGraph()
        positions = calculate_positions(lineage)
        assert positions == {}

    def test_single_node(self) -> None:
        lineage = LineageGraph(
            nodes={"db.X": TableNode("db", "X", "MergeTree")},
            edges=[],
        )
        positions = calculate_positions(lineage)
        assert "db.X" in positions

    def test_diamond_all_positioned(self, diamond_lineage: LineageGraph) -> None:
        positions = calculate_positions(diamond_lineage)
        assert set(positions.keys()) == set(diamond_lineage.nodes.keys())

    def test_cycle_does_not_raise(self) -> None:
        """Cyclic graph must not cause infinite recursion."""
        nodes = {
            "db.A": TableNode("db", "A", "MergeTree"),
            "db.B": TableNode("db", "B", "MergeTree"),
        }
        edges = [
            LineageEdge("db.A", "db.B", "mv"),
            LineageEdge("db.B", "db.A", "mv"),
        ]
        lineage = LineageGraph(nodes=nodes, edges=edges)
        positions = calculate_positions(lineage)  # should not raise
        assert len(positions) == 2

    def test_x_spacing_between_levels(self, linear_lineage: LineageGraph) -> None:
        """Adjacent levels must be exactly x_spacing (320) apart."""
        positions = calculate_positions(linear_lineage)
        x_diff = positions["db.MV"][0] - positions["db.A"][0]
        assert x_diff == pytest.approx(380.0)


class TestGetConnectedSubgraph:
    def test_returns_all_nodes_in_linear_chain(
        self, linear_lineage: LineageGraph
    ) -> None:
        connected = get_connected_subgraph(linear_lineage, "db.MV")
        assert connected == {"db.A", "db.MV", "db.B"}

    def test_includes_selected_node(self, linear_lineage: LineageGraph) -> None:
        connected = get_connected_subgraph(linear_lineage, "db.A")
        assert "db.A" in connected

    def test_isolated_node(self) -> None:
        lineage = LineageGraph(
            nodes={
                "db.X": TableNode("db", "X", "MergeTree"),
                "db.Y": TableNode("db", "Y", "MergeTree"),
            },
            edges=[],
        )
        connected = get_connected_subgraph(lineage, "db.X")
        assert connected == {"db.X"}

    def test_source_node_reaches_downstream(self, linear_lineage: LineageGraph) -> None:
        connected = get_connected_subgraph(linear_lineage, "db.A")
        assert "db.B" in connected

    def test_target_node_reaches_upstream(self, linear_lineage: LineageGraph) -> None:
        connected = get_connected_subgraph(linear_lineage, "db.B")
        assert "db.A" in connected

    def test_diamond_from_source(self, diamond_lineage: LineageGraph) -> None:
        connected = get_connected_subgraph(diamond_lineage, "db.A")
        # All nodes reachable from db.A downstream
        assert "db.MV1" in connected
        assert "db.C" in connected

    def test_diamond_from_sink(self, diamond_lineage: LineageGraph) -> None:
        connected = get_connected_subgraph(diamond_lineage, "db.C")
        # Both upstream paths reachable
        assert "db.A" in connected
        assert "db.B" in connected
        assert "db.MV1" in connected
        assert "db.MV2" in connected
