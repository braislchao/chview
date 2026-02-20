"""SQL parsing utilities for extracting table references from CREATE statements."""

import re


def _qualify_table_name(table_ref: str, default_database: str) -> str:
    """Qualify a table reference with a database prefix if not already qualified.

    Args:
        table_ref: Table reference, possibly unqualified
        default_database: Database to use if not already qualified

    Returns:
        Fully qualified table name as "database.table"
    """
    table_ref = table_ref.replace("`", "").strip()
    if "." in table_ref:
        return table_ref
    return f"{default_database}.{table_ref}"


def parse_source_tables(create_query: str, mv_database: str) -> list[str]:
    """Parse source tables from a CREATE MATERIALIZED VIEW SQL statement.

    Extracts all FROM and JOIN references from the SELECT portion of the query.

    Args:
        create_query: Full CREATE MATERIALIZED VIEW SQL
        mv_database: Database of the MV (used to qualify unqualified table refs)

    Returns:
        Sorted list of fully qualified source table names
    """
    match = re.search(r"\bAS\s+SELECT\b", create_query, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    select_part = create_query[match.start() :]
    table_pattern = r"(?:`[^`]+`|[a-zA-Z_]\w*)(?:\.(?:`[^`]+`|[a-zA-Z_]\w*))?"
    sources: set[str] = set()

    from_pattern = rf"\bFROM\s+({table_pattern})"
    for m in re.finditer(from_pattern, select_part, re.IGNORECASE):
        sources.add(_qualify_table_name(m.group(1), mv_database))

    join_pattern = rf"\bJOIN\s+({table_pattern})"
    for m in re.finditer(join_pattern, select_part, re.IGNORECASE):
        sources.add(_qualify_table_name(m.group(1), mv_database))

    return sorted(sources)


def parse_target_table(
    create_query: str, mv_database: str, mv_name: str
) -> tuple[str, bool]:
    """Parse the target table from a CREATE MATERIALIZED VIEW SQL statement.

    Args:
        create_query: Full CREATE MATERIALIZED VIEW SQL
        mv_database: Database of the MV
        mv_name: Name of the MV (used for implicit inner table name)

    Returns:
        Tuple of (fully_qualified_target_name, is_implicit)
        where is_implicit=True means the MV uses an inner/implicit storage table
    """
    table_pattern = r"(?:`[^`]+`|[a-zA-Z_]\w*)(?:\.(?:`[^`]+`|[a-zA-Z_]\w*))?"
    to_match = re.search(rf"\bTO\s+({table_pattern})", create_query, re.IGNORECASE)
    if to_match:
        return _qualify_table_name(to_match.group(1), mv_database), False
    return f"{mv_database}.`.inner.{mv_name}`", True
