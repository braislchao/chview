"""Unit tests for chview.lineage.parser."""

from chview.lineage.parser import (
    _qualify_table_name,
    parse_source_tables,
    parse_target_table,
)


class TestQualifyTableName:
    def test_already_qualified(self) -> None:
        assert _qualify_table_name("mydb.mytable", "other") == "mydb.mytable"

    def test_unqualified_adds_default(self) -> None:
        assert _qualify_table_name("mytable", "mydb") == "mydb.mytable"

    def test_strips_backticks(self) -> None:
        assert _qualify_table_name("`mydb`.`mytable`", "other") == "mydb.mytable"

    def test_strips_surrounding_whitespace(self) -> None:
        assert _qualify_table_name("  mytable  ", "mydb") == "mydb.mytable"

    def test_backtick_qualified(self) -> None:
        result = _qualify_table_name("`mydb`.`t`", "fallback")
        assert result == "mydb.t"


class TestParseSourceTables:
    def test_simple_from(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO db.out AS SELECT id FROM db.events"
        sources = parse_source_tables(sql, "db")
        assert "db.events" in sources

    def test_qualified_from(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO db.out AS SELECT id FROM other.events"
        sources = parse_source_tables(sql, "db")
        assert "other.events" in sources

    def test_join(self) -> None:
        sql = (
            "CREATE MATERIALIZED VIEW db.mv TO db.out AS "
            "SELECT a.id FROM db.tableA a JOIN db.tableB b ON a.id = b.id"
        )
        sources = parse_source_tables(sql, "db")
        assert "db.tableA" in sources
        assert "db.tableB" in sources

    def test_no_select_returns_empty(self) -> None:
        sql = "CREATE TABLE db.foo (id UInt64) ENGINE = MergeTree()"
        sources = parse_source_tables(sql, "db")
        assert sources == []

    def test_unqualified_table_qualified_with_mv_db(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO db.out AS SELECT id FROM raw_events"
        sources = parse_source_tables(sql, "db")
        assert "db.raw_events" in sources

    def test_returns_sorted_list(self) -> None:
        sql = (
            "CREATE MATERIALIZED VIEW db.mv TO db.out AS "
            "SELECT id FROM db.zzz JOIN db.aaa ON 1=1"
        )
        sources = parse_source_tables(sql, "db")
        assert sources == sorted(sources)

    def test_deduplicates_same_table(self) -> None:
        sql = (
            "CREATE MATERIALIZED VIEW db.mv TO db.out AS "
            "SELECT id FROM db.src JOIN db.src s2 ON 1=1"
        )
        sources = parse_source_tables(sql, "db")
        assert sources.count("db.src") == 1

    def test_case_insensitive_as_select(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO db.out as select id from db.tbl"
        sources = parse_source_tables(sql, "db")
        assert "db.tbl" in sources


class TestParseTargetTable:
    def test_explicit_to_clause(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO db.output AS SELECT id FROM db.src"
        target, is_implicit = parse_target_table(sql, "db", "mv")
        assert target == "db.output"
        assert is_implicit is False

    def test_qualified_to_clause(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv TO other.output AS SELECT id FROM db.src"
        target, is_implicit = parse_target_table(sql, "db", "mv")
        assert target == "other.output"
        assert is_implicit is False

    def test_no_to_clause_returns_implicit(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv AS SELECT id FROM db.src"
        target, is_implicit = parse_target_table(sql, "db", "mv")
        assert is_implicit is True
        assert ".inner." in target
        assert "mv" in target

    def test_implicit_target_uses_mv_database(self) -> None:
        sql = "CREATE MATERIALIZED VIEW db.mv AS SELECT id FROM db.src"
        target, _ = parse_target_table(sql, "db", "mv")
        assert target.startswith("db.")

    def test_case_insensitive_to(self) -> None:
        sql = "create materialized view db.mv to db.sink as select id from db.src"
        target, is_implicit = parse_target_table(sql, "db", "mv")
        assert target == "db.sink"
        assert is_implicit is False
