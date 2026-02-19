"""Lineage graph construction from materialized view metadata."""

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from chview.lineage.parser import parse_source_tables, parse_target_table


@dataclass
class TableNode:
    """Represents a table or materialized view in the lineage graph."""

    database: str
    name: str
    engine: str
    full_name: str = ""

    def __post_init__(self) -> None:
        if not self.full_name:
            self.full_name = f"{self.database}.{self.name}"


@dataclass
class LineageEdge:
    """Represents a data flow edge in the lineage graph."""

    source: str
    target: str
    mv_name: str


@dataclass
class LineageGraph:
    """Complete lineage graph with nodes and edges."""

    nodes: dict[str, TableNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)
    mv_names: set[str] = field(default_factory=set)
    target_names: set[str] = field(default_factory=set)


def build_lineage(
    mv_df: pd.DataFrame, schema_df: Optional[pd.DataFrame] = None
) -> LineageGraph:
    """Build a lineage graph from materialized view metadata.

    Args:
        mv_df: DataFrame with MV info (database, name, create_table_query,
               dependencies_database, dependencies_table)
        schema_df: Optional DataFrame with table schema info (database, name, engine)

    Returns:
        Fully populated LineageGraph
    """
    lineage = LineageGraph()

    # Build lookup for actual engine types from schema
    engine_lookup: dict[str, str] = {}
    if schema_df is not None and not schema_df.empty:
        for _, row in schema_df.iterrows():
            full_name = f"{row['database']}.{row['name']}"
            engine_lookup[full_name] = row.get("engine", "Unknown")

    for _, row in mv_df.iterrows():
        db = row["database"]
        mv_name = row["name"]
        create_query = row["create_table_query"]
        mv_full_name = f"{db}.{mv_name}"

        lineage.nodes[mv_full_name] = TableNode(db, mv_name, "MaterializedView")
        lineage.mv_names.add(mv_full_name)

        sources = parse_source_tables(create_query, db)

        # Merge explicit ClickHouse dependency metadata
        deps_db = row.get("dependencies_database", [])
        deps_table = row.get("dependencies_table", [])
        if isinstance(deps_db, (list, tuple)) and isinstance(deps_table, (list, tuple)):
            for dep_db, dep_tbl in zip(deps_db, deps_table):
                dep_full = f"{dep_db}.{dep_tbl}"
                if dep_full not in sources:
                    sources.append(dep_full)

        for source in sources:
            if source not in lineage.nodes:
                parts = source.split(".", 1)
                source_db = parts[0] if len(parts) > 1 else db
                source_name = parts[1] if len(parts) > 1 else parts[0]
                actual_engine = engine_lookup.get(source, "Source")
                lineage.nodes[source] = TableNode(source_db, source_name, actual_engine)
            lineage.edges.append(LineageEdge(source, mv_full_name, mv_full_name))

        target, is_implicit = parse_target_table(create_query, db, mv_name)
        if target not in lineage.nodes:
            parts = target.split(".", 1)
            target_db = parts[0] if len(parts) > 1 else db
            target_name = parts[1] if len(parts) > 1 else parts[0]
            actual_engine = engine_lookup.get(target)
            if actual_engine is None:
                actual_engine = "implicit" if is_implicit else "target"
            lineage.nodes[target] = TableNode(target_db, target_name, actual_engine)
        lineage.target_names.add(target)
        lineage.edges.append(LineageEdge(mv_full_name, target, mv_full_name))

    return lineage
