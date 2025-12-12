"""Tests for StarRocks Engine Adapter

This test suite covers the StarRocks-specific functionality of the engine adapter,
including schema operations, table operations, and StarRocks-specific table properties.

Test classes are organized by functionality (following the standard order):
- TestSchemaOperations: Schema/Database operations
- TestTableOperations: Basic table operations
- TestKeyPropertyBuilding: Table key types (primary_key, duplicate_key, unique_key, aggregate_key)
- TestPartitionPropertyBuilding: Partition (partitioned_by, partitions)
- TestDistributionPropertyBuilding: Distribution (distributed_by)
- TestOrderByPropertyBuilding: Order By (order_by, clustered_by)
- TestCommentPropertyBuilding: Comments (table and column)
- TestGenericPropertyBuilding: Generic properties (replication_num, etc.)
- TestComprehensive: Comprehensive tests with all features combined

Unit tests use @pytest.mark.parametrize to systematically cover all value forms.
"""

import typing as t

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import load_sql_based_model, SqlModel
from pytest_mock.plugin import MockerFixture

pytestmark = [pytest.mark.starrocks, pytest.mark.engine]


# =============================================================================
# Schema Operations
# =============================================================================


class TestSchemaOperations:
    """Tests for schema (database) operations."""

    def test_create_schema(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE DATABASE statement generation.

        StarRocks uses DATABASE keyword (MySQL-style) instead of SCHEMA.
        """
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_schema("test_schema")

        assert to_sql_calls(adapter) == [
            "CREATE SCHEMA IF NOT EXISTS `test_schema`",
        ]

    def test_create_schema_without_if_exists(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE DATABASE without IF NOT EXISTS clause."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_schema("test_schema", ignore_if_exists=False)

        assert to_sql_calls(adapter) == [
            "CREATE SCHEMA `test_schema`",
        ]

    def test_drop_schema(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test DROP DATABASE statement generation."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.drop_schema("test_schema")
        adapter.drop_schema("test_schema", ignore_if_not_exists=False)

        assert to_sql_calls(adapter) == [
            "DROP SCHEMA IF EXISTS `test_schema`",
            "DROP SCHEMA `test_schema`",
        ]


# =============================================================================
# Basic Table Operations
# =============================================================================


class TestTableOperations:
    """Tests for basic table operations."""

    def test_create_table(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test basic CREATE TABLE statement generation."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            "test_table",
            target_columns_to_types={
                "a": exp.DataType.build("INT"),
                "b": exp.DataType.build("VARCHAR(100)"),
            },
        )

        sql = to_sql_calls(adapter)[0]
        assert "CREATE TABLE IF NOT EXISTS `test_table`" in sql
        assert "`a` INT" in sql
        assert "`b` VARCHAR(100)" in sql

    def test_create_table_like(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE TABLE LIKE statement."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

        # Mock the columns() method to avoid database dependency
        from unittest.mock import MagicMock

        adapter.columns = MagicMock(return_value={"a": exp.DataType.build("INT")})

        adapter.create_table_like("target_table", "source_table")

        # Verify columns() was called to get source table structure
        adapter.columns.assert_called_once_with("source_table")

        # Verify create_table was called with the columns from source table
        sql = to_sql_calls(adapter)[0]
        assert "CREATE TABLE IF NOT EXISTS `target_table`" in sql
        assert "`a` INT" in sql

    def test_rename_table(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test RENAME TABLE statement."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

        # Test 1: Simple table names (no database qualifier)
        adapter.rename_table("old_table", "new_table")
        adapter.cursor.execute.assert_called_with(
            "ALTER TABLE `old_table` RENAME `new_table`"
        )

        # Test 2: Database-qualified names - RENAME only uses table name
        adapter.cursor.execute.reset_mock()
        adapter.rename_table("db.old_table", "db.new_table")
        # StarRocks RENAME clause requires unqualified table name
        adapter.cursor.execute.assert_called_with(
            "ALTER TABLE `db`.`old_table` RENAME `new_table`"
        )

    def test_delete_from(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test DELETE statement generation."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.delete_from(exp.to_table("test_table"), "id = 1")

        assert to_sql_calls(adapter) == [
            "DELETE FROM `test_table` WHERE `id` = 1",
        ]

    def test_create_index(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE INDEX statement - StarRocks doesn't support standalone indexes."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_index("test_table", "idx_name", ("cola",))

        # StarRocks skips index creation - verify no execute call was made
        adapter.cursor.execute.assert_not_called()

    def test_create_view(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE VIEW statement generation."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
        adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)

        assert to_sql_calls(adapter) == [
            "CREATE OR REPLACE VIEW `test_view` AS SELECT `a` FROM `tbl`",
            "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        ]

    def test_delete_where_true_optimization(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """
        Test DELETE with WHERE TRUE optimization.

        WHERE TRUE is converted to TRUNCATE TABLE for better performance.
        This works for all StarRocks table types and is semantically equivalent.
        """
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

        # Test WHERE TRUE
        adapter.delete_from(exp.to_table("test_table"), exp.true())
        assert to_sql_calls(adapter) == [
            "TRUNCATE TABLE `test_table`",
        ]

        adapter.cursor.reset_mock()

        # Test no WHERE clause (also uses TRUNCATE)
        adapter.delete_from(exp.to_table("test_table"), None)
        assert to_sql_calls(adapter) == [
            "TRUNCATE TABLE `test_table`",
        ]


# =============================================================================
# Key Property Building
# =============================================================================


class TestKeyPropertyBuilding:
    """
    Tests for table key types: primary_key, duplicate_key, unique_key, aggregate_key.

    Key columns must be the first N columns in the table definition.
    Tests parse actual Model SQL to ensure real-world compatibility.
    """

    @pytest.mark.parametrize(
        "key_type,key_value,expected_clause",
        [
            # primary_key - single column
            ("primary_key", "id", "PRIMARY KEY (`id`)"),
            # primary_key - tuple form (multi-column)
            ("primary_key", "(id, dt)", "PRIMARY KEY (`id`, `dt`)"),
            # duplicate_key - tuple form
            ("duplicate_key", "(id, name)", "DUPLICATE KEY (`id`, `name`)"),
            # unique_key - tuple form
            ("unique_key", "(id, dt)", "UNIQUE KEY (`id`, `dt`)"),
            # aggregate_key - multi-column. not supported (requires aggregation function specification)
            # ("aggregate_key", ("id", "dt"), "AGGREGATE KEY (`id`, `dt`)"),
        ],
    )
    def test_key_types_with_tuple_form(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        key_type: str,
        key_value: str,
        expected_clause: str,
    ):
        """Test key types with tuple form: (id, dt) parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE, name STRING, value DECIMAL(10,2)),
            physical_properties (
                {key_type} = {key_value}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql

    @pytest.mark.parametrize(
        "key_string,expected_clause",
        [
            # String with parentheses
            ('"(id, dt)"', "PRIMARY KEY (`id`, `dt`)"),
            # String without parentheses (auto-wrapped)
            ('"id, dt"', "PRIMARY KEY (`id`, `dt`)"),
            # Single column string
            ('"id"', "PRIMARY KEY (`id`)"),
        ],
    )
    def test_primary_key_string_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        key_string: str,
        expected_clause: str,
    ):
        """Test primary_key with string forms (with/without parentheses) parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE, value DECIMAL(10,2)),
            physical_properties (
                primary_key = {key_string}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql

    def test_primary_key_single_identifier(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test primary_key = id (single identifier without quotes)."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE),
            physical_properties (
                primary_key = id
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PRIMARY KEY (`id`)" in sql

    def test_primary_key_via_table_properties_tuple(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test primary_key passed via physical_properties with tuple form - duplicate of test_key_types_with_tuple_form."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE, value DECIMAL(10,2)),
            physical_properties (
                primary_key = (id, dt)
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PRIMARY KEY (`id`, `dt`)" in sql

    def test_column_reordering_for_key(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test column reordering for key tables.

        StarRocks Requirement:
        Key columns MUST be the first N columns in CREATE TABLE statement.
        """
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

        columns_to_types = {
            "customer_id": exp.DataType.build("INT"),
            "region": exp.DataType.build("VARCHAR(50)"),
            "order_id": exp.DataType.build("BIGINT"),
            "event_date": exp.DataType.build("DATE"),
            "amount": exp.DataType.build("DECIMAL(18,2)"),
        }

        adapter.create_table(
            "test_table",
            target_columns_to_types=columns_to_types,
            primary_key=("order_id", "event_date"),
        )

        sql = to_sql_calls(adapter)[0]
        assert "PRIMARY KEY (`order_id`, `event_date`)" in sql

        import re

        col_match = re.search(r"CREATE TABLE.*?\((.*)\)\s*PRIMARY KEY", sql, re.DOTALL)
        assert col_match, "Could not extract column definitions"
        col_defs = col_match.group(1)

        order_id_pos = col_defs.find("`order_id`")
        event_date_pos = col_defs.find("`event_date`")
        customer_id_pos = col_defs.find("`customer_id`")

        assert order_id_pos < event_date_pos, "order_id must appear before event_date"
        assert (
            event_date_pos < customer_id_pos
        ), "event_date must appear before customer_id"


# =============================================================================
# Partition Property Building
# =============================================================================


class TestPartitionPropertyBuilding:
    """Tests for partitioned_by/partition_by and partitions properties."""

    @pytest.mark.parametrize(
        "partition_expr,expected_clause",
        [
            # Expression partitioning - single column
            ("'dt'", "PARTITION BY dt"),
            # Expression partitioning - multi-column
            ("(year, month)", "PARTITION BY year, month"),
            # RANGE partitioning
            ("RANGE (dt)", "PARTITION BY RANGE (`dt`) ()"),
            # LIST partitioning
            ("LIST (region)", "PARTITION BY LIST (`region`) ()"),
        ],
    )
    def test_partitioned_by_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        partition_expr: str,
        expected_clause: str,
    ):
        """Test partition_by with various forms parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE, year INT, month INT, region STRING),
            physical_properties (
                partition_by = {partition_expr}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            partitioned_by=model.partitioned_by,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql

    def test_partition_by_alias(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test partition_by as alias for partitioned_by in physical_properties."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, year INT, month INT),
            physical_properties (
                partition_by = (year, month)
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            partitioned_by=model.partitioned_by,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PARTITION BY year, month" in sql

    def test_partitioned_by_as_model_parameter(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test partitioned_by as model-level parameter (not in physical_properties)."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, year INT, month INT, value DECIMAL(10,2)),
            partitioned_by (year, month)
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            partitioned_by=model.partitioned_by,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PARTITION BY year, month" in sql

    def test_partitions_value_forms(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test partitions property with single and multiple partition definitions."""
        # Single partition string (paren)
        model_sql_single = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE),
            physical_properties (
                partition_by = RANGE(dt),
                partitions = 'PARTITION p1 VALUES LESS THAN ("2024-01-01")'
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql_single, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            partitioned_by=model.partitioned_by,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PARTITION p1" in sql
        assert "VALUES LESS THAN" in sql

        # Multiple partitions (tuple of strings)
        model_sql_multiple = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, dt DATE),
            physical_properties (
                partition_by = RANGE(dt),
                partitions = (
                    'PARTITION p1 VALUES LESS THAN ("2024-01-01")',
                    'PARTITION p2 VALUES LESS THAN ("2024-02-01")'
                )
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql_multiple, default_dialect="starrocks")
        model = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            partitioned_by=model.partitioned_by,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "PARTITION p1" in sql
        assert "PARTITION p2" in sql


# =============================================================================
# Distribution Property Building
# =============================================================================


class TestDistributionPropertyBuilding:
    """Tests for distributed_by property."""

    @pytest.mark.parametrize(
        "dist_input,expected_clause",
        [
            # String form: HASH single column
            ('"HASH(id) BUCKETS 10"', "DISTRIBUTED BY HASH (`id`) BUCKETS 10"),
            # String form: HASH multi-column
            (
                '"HASH(id, region) BUCKETS 16"',
                "DISTRIBUTED BY HASH (`id`, `region`) BUCKETS 16",
            ),
            # String form: RANDOM
            ('"RANDOM"', "DISTRIBUTED BY RANDOM"),
            # String form: RANDOM with BUCKETS
            ('"RANDOM BUCKETS 10"', "DISTRIBUTED BY RANDOM BUCKETS 10"),
        ],
    )
    def test_distributed_by_string_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        dist_input: str,
        expected_clause: str,
    ):
        """Test distributed_by with string forms parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, region STRING),
            physical_properties (
                distributed_by = {dist_input}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql

    @pytest.mark.parametrize(
        "dist_struct,expected_clause",
        [
            # Structured: HASH with quoted kind
            ("(kind='HASH', expressions=id, buckets=32)", "DISTRIBUTED BY HASH (`id`) BUCKETS 32"),
            # Structured: HASH with unquoted kind (Column)
            ("(kind=HASH, expressions=id, buckets=10)", "DISTRIBUTED BY HASH (`id`) BUCKETS 10"),
            # Structured: HASH multi-column
            (
                "(kind='HASH', expressions=(a, b), buckets=16)",
                "DISTRIBUTED BY HASH (`a`, `b`) BUCKETS 16",
            ),
            # Structured: RANDOM
            ("(kind='RANDOM')", "DISTRIBUTED BY RANDOM"),
            # Structured: RANDOM with buckets
            ("(kind=RANDOM, buckets=10)", "DISTRIBUTED BY RANDOM BUCKETS 10"),
        ],
    )
    def test_distributed_by_structured_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        dist_struct: str,
        expected_clause: str,
    ):
        """Test distributed_by with structured tuple forms parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, a INT, b STRING, region STRING),
            physical_properties (
                distributed_by = {dist_struct}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql


# =============================================================================
# Order By Property Building
# =============================================================================


class TestOrderByPropertyBuilding:
    """Tests for order_by and clustered_by properties."""

    @pytest.mark.parametrize(
        "order_value,expected_clause,description",
        [
            # String form (double-quoted string)
            ('"id"', "ORDER BY (`id`)", "Bare string: single column"),
            (
                '"id, timestamp"',
                "ORDER BY (`id`, `timestamp`)",
                "Bare string: multi-column without parens",
            ),
            ('"(id, timestamp)"', "ORDER BY (`id`, `timestamp`)", "String with parens"),
            # Literal form (single-quoted string)
            ("'id'", "ORDER BY (`id`)", "Bare string: single column"),
            (
                "'id, timestamp'",
                "ORDER BY (`id`, `timestamp`)",
                "Bare string: multi-column without parens",
            ),
            ("'(id, timestamp)'", "ORDER BY (`id`, `timestamp`)", "String with parens"),
            # Tuple form (direct expression construction in MODEL)
            ("(id, timestamp)", "ORDER BY (`id`, `timestamp`)", "Tuple: multi-column"),
            # Single identifier (unquoted)
            ("id", "ORDER BY (`id`)", "Identifier: single column"),
        ],
    )
    def test_order_by_value_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        order_value: str,
        expected_clause: str,
        description: str,
    ):
        """Test ORDER BY with various input forms parsed from physical_properties."""
        model_sql = f"""
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, timestamp DATETIME, value DECIMAL(10,2)),
            physical_properties (
                order_by = {order_value}
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_clause in sql, (
            f"\nTest case: {description}\n"
            f"Input: {order_value}\n"
            f"Expected: {expected_clause}\n"
            f"Actual SQL: {sql}"
        )

    def test_clustered_by_generates_order_by(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test that clustered_by parameter generates ORDER BY clause."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, timestamp DATETIME, value DECIMAL(10,2)),
            physical_properties (
                clustered_by = (id, timestamp)
            )
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            table_properties=model.physical_properties,
        )

        sql = to_sql_calls(adapter)[0]
        assert "ORDER BY (`id`, `timestamp`)" in sql
        assert "CLUSTER BY" not in sql

    def test_clustered_by_as_model_parameter(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test clustered_by as model-level parameter (not in physical_properties)."""
        model_sql = """
        MODEL (
            name t,
            kind FULL,
            dialect starrocks,
            columns (id INT, timestamp DATETIME, value DECIMAL(10,2)),
            clustered_by id
        );
        SELECT 1;
        """

        parsed = parse(model_sql, default_dialect="starrocks")
        model: SqlModel = t.cast(SqlModel, load_sql_based_model(parsed))

        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            model.name,
            model.columns_to_types,
            clustered_by=model.clustered_by,
        )

        sql = to_sql_calls(adapter)[0]
        assert "ORDER BY (`id`)" in sql
        # Verify that StarRocks uses ORDER BY, not CLUSTER BY
        assert "CLUSTER BY" not in sql


# =============================================================================
# Generic Property Building
# =============================================================================


class TestGenericPropertyBuilding:
    """Tests for generic table properties (replication_num, etc.)."""

    @pytest.mark.parametrize(
        "prop_name,prop_value,expected_in_sql",
        [
            # Integer value
            ("replication_num", "1", "'replication_num'='1'"),
            ("replication_num", "3", "'replication_num'='3'"),
            # Boolean TRUE
            ("enable_persistent_index", "TRUE", "'enable_persistent_index'='TRUE'"),
            # Boolean FALSE
            ("in_memory", "FALSE", "'in_memory'='FALSE'"),
            # String value
            ("compression", "LZ4", "'compression'='LZ4'"),
        ],
    )
    def test_generic_property_value_forms(
        self,
        make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter],
        prop_name: str,
        prop_value: str,
        expected_in_sql: str,
    ):
        """Test generic properties with various value types."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)

        adapter.create_table(
            "test_table",
            target_columns_to_types={
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR(100)"),
            },
            primary_key=("id",),
            table_properties={
                prop_name: prop_value,
            },
        )

        sql = to_sql_calls(adapter)[0]
        assert expected_in_sql in sql


# =============================================================================
# Comment Property Building
# =============================================================================


class TestCommentPropertyBuilding:
    """Tests for table and column comments."""

    def test_table_and_column_comments(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE TABLE with table and column comments."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            "test_table",
            target_columns_to_types={
                "a": exp.DataType.build("INT"),
                "b": exp.DataType.build("VARCHAR(100)"),
            },
            table_description="Test table description",
            column_descriptions={
                "a": "Column A description",
                "b": "Column B description",
            },
        )

        sql = to_sql_calls(adapter)[0]
        assert "COMMENT 'Test table description'" in sql
        assert "COMMENT 'Column A description'" in sql
        assert "COMMENT 'Column B description'" in sql

    def test_view_with_comments(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """Test CREATE VIEW with comments."""
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_view(
            "test_view",
            parse_one("SELECT a FROM tbl"),
            replace=False,
            target_columns_to_types={"a": exp.DataType.build("INT")},
            table_description="Test view description",
            column_descriptions={"a": "Column A description"},
        )

        sql = to_sql_calls(adapter)[0]
        assert "COMMENT 'Test view description'" in sql
        assert "COMMENT 'Column A description'" in sql


# =============================================================================
# Comprehensive Tests
# =============================================================================


class TestComprehensive:
    """Comprehensive tests combining multiple features."""

    def test_create_table_comprehensive(
        self, make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]
    ):
        """
        Test CREATE TABLE with all features combined:
        - PRIMARY KEY
        - Table and column comments
        - DISTRIBUTED BY
        - ORDER BY
        - Custom properties
        """
        adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
        adapter.create_table(
            "test_table",
            target_columns_to_types={
                "customer_id": exp.DataType.build("INT"),
                "order_id": exp.DataType.build("BIGINT"),
                "event_date": exp.DataType.build("DATE"),
                "amount": exp.DataType.build("DECIMAL(10,2)"),
            },
            primary_key=("order_id", "event_date"),
            table_description="Sales transaction table",
            column_descriptions={
                "customer_id": "Customer identifier",
                "order_id": "Order identifier",
            },
            table_properties={
                "distributed_by": exp.Tuple(
                    expressions=[
                        exp.EQ(
                            this=exp.Column(this="kind"),
                            expression=exp.Literal.string("HASH"),
                        ),
                        exp.EQ(
                            this=exp.Column(this="expressions"),
                            expression=exp.Tuple(
                                expressions=[exp.to_column("customer_id")]
                            ),
                        ),
                        exp.EQ(
                            this=exp.Column(this="buckets"),
                            expression=exp.Literal.number(10),
                        ),
                    ]
                ),
                "replication_num": "3",
            },
            clustered_by=[exp.to_column("customer_id"), exp.to_column("order_id")],
        )

        sql = to_sql_calls(adapter)[0]
        assert "CREATE TABLE IF NOT EXISTS `test_table`" in sql
        assert "PRIMARY KEY (`order_id`, `event_date`)" in sql
        assert "COMMENT 'Sales transaction table'" in sql
        assert "COMMENT 'Customer identifier'" in sql
        assert "COMMENT 'Order identifier'" in sql
        assert "DISTRIBUTED BY HASH (`customer_id`) BUCKETS 10" in sql
        assert "ORDER BY (`customer_id`, `order_id`)" in sql
        assert "PROPERTIES ('replication_num'='3')" in sql
