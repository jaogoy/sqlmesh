"""
Integration tests for StarRocks Engine Adapter

These tests require a running StarRocks instance.
They verify that the generated SQL actually works on real StarRocks database.

Strategy:
- Basic test: Verify fundamental functionality works
- Complex test: Verify comprehensive SQL with all features works

Run with:
  pytest -m "starrocks and docker" tests/core/engine_adapter/integration/test_integration_starrocks.py

Or against local StarRocks:
  export STARROCKS_HOST=localhost
  export STARROCKS_PORT=9030
  export STARROCKS_USER=root
  export STARROCKS_PASSWORD=""
  pytest tests/core/engine_adapter/integration/test_integration_starrocks.py
"""

import logging
import os
import typing as t
from functools import partial

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from sqlmesh.core.model.definition import load_sql_based_model
import sqlmesh.core.dialect as d

# Mark as docker test (can also run against local StarRocks)
# Remove 'docker' marker if you want to run against local instance only
pytestmark = [pytest.mark.starrocks, pytest.mark.docker, pytest.mark.engine]


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def starrocks_connection_config() -> t.Dict[str, t.Any]:
    """StarRocks connection configuration from environment variables."""
    return {
        "host": os.getenv("STARROCKS_HOST", "localhost"),
        "port": int(os.getenv("STARROCKS_PORT", "9030")),
        "user": os.getenv("STARROCKS_USER", "myname"),
        "password": os.getenv("STARROCKS_PASSWORD", "pswd1234"),
    }


@pytest.fixture(scope="module")
def starrocks_adapter(starrocks_connection_config) -> StarRocksEngineAdapter:
    """Create a real StarRocks adapter connected to database."""
    from pymysql import connect

    connection_factory = partial(connect, **starrocks_connection_config)
    adapter = StarRocksEngineAdapter(connection_factory)

    yield adapter

    # Cleanup: adapter will auto-close connection




class TestBasicOperations:
    """
    Basic Operations

    Each test method verifies one fundamental SQL operation.
    This allows running individual tests and clear failure reporting.
    """

    def test_create_drop_schema(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test CREATE DATABASE and DROP DATABASE."""
        db_name = "sr_test_create_drop_db"

        try:
            # CREATE DATABASE
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            result = starrocks_adapter.fetchone(
                f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
            )
            assert result is not None, "CREATE DATABASE failed"
            assert result[0] == db_name

            # DROP DATABASE
            starrocks_adapter.drop_schema(db_name)
            result = starrocks_adapter.fetchone(
                f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
            )
            assert result is None, "DROP DATABASE failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_create_drop_table(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test CREATE TABLE and DROP TABLE."""
        db_name = "sr_test_table_db"
        table_name = f"{db_name}.sr_test_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
                table_properties={
                    "properties": {
                        "replication_num": exp.Literal.string("1"),
                    },
                },
            )
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_test_table'"
            )
            assert result is not None, "CREATE TABLE failed"

            # DROP TABLE
            starrocks_adapter.drop_table(table_name)
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_test_table'"
            )
            assert result is None, "DROP TABLE failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_insert_select(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test INSERT and SELECT operations."""
        db_name = "sr_test_insert_db"
        table_name = f"{db_name}.sr_test_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
            )

            # INSERT
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
            )

            # SELECT
            results = starrocks_adapter.fetchall(
                f"SELECT id, name FROM {table_name} ORDER BY id"
            )
            assert len(results) == 2, "INSERT/SELECT failed"
            assert results[0] == (1, "Alice")
            assert results[1] == (2, "Bob")
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_update(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test UPDATE operation."""
        db_name = "sr_test_update_db"
        table_name = f"{db_name}.sr_test_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
                table_properties={"primary_key": "id"},
            )
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')"
            )

            # UPDATE
            starrocks_adapter.execute(
                f"UPDATE {table_name} SET name = 'Alice Updated' WHERE id = 1"
            )
            result = starrocks_adapter.fetchone(
                f"SELECT name FROM {table_name} WHERE id = 1"
            )
            assert result[0] == "Alice Updated", "UPDATE failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_delete(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test DELETE operation."""
        db_name = "sr_test_delete_db"
        table_name = f"{db_name}.sr_test_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
            )
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
            )

            # DELETE
            starrocks_adapter.delete_from(exp.to_table(table_name), "id = 2")
            count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {table_name}")
            assert count[0] == 1, "DELETE failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_rename_table(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test RENAME TABLE operation."""
        db_name = "sr_test_rename_db"
        old_table = f"{db_name}.old_table"
        new_table = f"{db_name}.new_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                old_table,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
            )

            # Insert test data
            starrocks_adapter.execute(
                f"INSERT INTO {old_table} (id, name) VALUES (1, 'Test')"
            )

            # RENAME TABLE
            starrocks_adapter.rename_table(old_table, new_table)

            # Verify old table doesn't exist
            old_exists = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'old_table'"
            )
            assert old_exists is None, "Old table should not exist after rename"

            # Verify new table exists
            new_exists = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'new_table'"
            )
            assert new_exists is not None, "New table should exist after rename"

            # Verify data is preserved
            count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {new_table}")
            assert count[0] == 1, "Data should be preserved after rename"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_create_index(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test CREATE INDEX operation (should be skipped for StarRocks).

        StarRocks doesn't support standalone CREATE INDEX statements.
        The adapter should skip index creation without errors.
        """
        db_name = "sr_test_index_db"
        table_name = f"{db_name}.sr_test_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
            )

            # CREATE INDEX (should be skipped silently)
            starrocks_adapter.create_index(table_name, "idx_name", ("name",))

            # Verify table still exists and is functional
            count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {table_name}")
            assert (
                count is not None
            ), "Table should still be functional after skipped index creation"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_create_drop_view(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test CREATE VIEW and DROP VIEW."""
        db_name = "sr_test_view_db"
        table_name = f"{db_name}.sr_test_table"
        view_name = f"{db_name}.sr_test_view"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
            )

            # CREATE VIEW
            starrocks_adapter.create_view(
                view_name,
                parse_one(f"SELECT id, name FROM {table_name}"),
            )
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.VIEWS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_test_view'"
            )
            assert result is not None, "CREATE VIEW failed"

            # DROP VIEW
            starrocks_adapter.drop_view(view_name)
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.VIEWS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_test_view'"
            )
            assert result is None, "DROP VIEW failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)


class TestTableFeatures:
    """
    Table Features

    Each test method verifies one CREATE TABLE feature that is NOT covered by E2E tests.
    Focus on independent functionality like comments and data type compatibility.
    """

    def test_table_and_column_comments(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test table and column comments."""
        db_name = "sr_test_comment_db"
        table_name = f"{db_name}.sr_comment_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with comments
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
                table_description="Test table comment",
                column_descriptions={
                    "id": "User ID",
                    "name": "User name",
                },
            )

            # Verify table comment
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_COMMENT FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_comment_table'"
            )
            assert result[0] == "Test table comment", "Table comment not set"

            # Verify column comments
            columns = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_comment_table' "
                f"ORDER BY ORDINAL_POSITION"
            )
            column_comments = {row[0]: row[1] for row in columns}
            assert column_comments["id"] == "User ID"
            assert column_comments["name"] == "User name"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_multiple_data_types(self, starrocks_adapter: StarRocksEngineAdapter):
        """
        Test basic data types support.

        Covers: numeric, string, datetime, boolean, and JSON types with precision.
        Reference: https://docs.starrocks.io/docs/sql-reference/data-types/
        """
        db_name = "sr_test_types_db"
        table_name = f"{db_name}.sr_types_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with multiple data types
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    # Numeric types
                    "col_tinyint": exp.DataType.build("TINYINT"),
                    "col_smallint": exp.DataType.build("SMALLINT"),
                    "col_int": exp.DataType.build("INT"),
                    "col_bigint": exp.DataType.build("BIGINT"),
                    "col_float": exp.DataType.build("FLOAT"),
                    "col_double": exp.DataType.build("DOUBLE"),
                    "col_decimal": exp.DataType.build("DECIMAL(18,2)"),
                    # String types with precision
                    "col_char": exp.DataType.build("CHAR(10)"),
                    "col_varchar": exp.DataType.build("VARCHAR(200)"),
                    "col_string": exp.DataType.build("STRING"),
                    # Date/Time types
                    "col_date": exp.DataType.build("DATE"),
                    "col_datetime": exp.DataType.build("DATETIME"),
                    # Boolean and JSON
                    "col_boolean": exp.DataType.build("BOOLEAN"),
                    "col_json": exp.DataType.build("JSON"),
                },
            )

            # Verify all columns created with correct types
            columns = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_types_table' "
                f"ORDER BY ORDINAL_POSITION"
            )
            assert len(columns) == 14, f"Expected 14 columns, got {len(columns)}"

            # Test data insertion with various types
            starrocks_adapter.execute(
                f"""
                INSERT INTO {table_name}
                (col_tinyint, col_smallint, col_int, col_bigint, col_float, col_double, col_decimal,
                 col_char, col_varchar, col_string, col_date, col_datetime, col_boolean, col_json)
                VALUES
                (127, 32767, 2147483647, 9223372036854775807, 3.14, 3.141592653589793, 12345.67,
                 'test', 'test varchar', 'test string', '2024-01-01', '2024-01-01 12:00:00',
                 true, '{{"key": "value"}}')
                """
            )

            # Verify insertion
            count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {table_name}")
            assert count[0] == 1, "Data insertion with basic types failed"

            # Verify data retrieval
            result = starrocks_adapter.fetchone(
                f"SELECT col_int, col_varchar, col_date FROM {table_name}"
            )
            assert result[0] == 2147483647
            assert result[1] == "test varchar"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # @pytest.mark.skip(reason="Complex types (ARRAY/MAP/STRUCT) may not be fully supported yet")
    def test_complex_data_types(self, starrocks_adapter: StarRocksEngineAdapter):
        """
        Test complex and nested data types support (ARRAY, MAP, STRUCT).

        Covers:
        - Simple complex types: ARRAY<INT>, MAP<STRING,INT>, STRUCT<a INT, b STRING>
        - Nested ARRAY: ARRAY<ARRAY<INT>>
        - Nested MAP: MAP<STRING, ARRAY<INT>>
        - Nested STRUCT: STRUCT<id INT, tags ARRAY<STRING>, metadata MAP<STRING,INT>>
        - Mixed nesting: ARRAY<STRUCT<id INT, name STRING>>
        - Deep nesting: MAP<STRING, ARRAY<STRUCT<field1 INT, field2 STRING>>>

        Note: These types are available in StarRocks 2.5+ but may require additional
        configuration or may not be fully supported in the current adapter.
        Reference: https://docs.starrocks.io/docs/sql-reference/data-types/
        """
        db_name = "sr_test_complex_types_db"
        table_name = f"{db_name}.sr_complex_types_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with complex and nested data types
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("BIGINT"),
                    # Simple complex types
                    "col_array_simple": exp.DataType.build("ARRAY<INT>"),
                    "col_map_simple": exp.DataType.build("MAP<STRING,INT>"),
                    "col_struct_simple": exp.DataType.build("STRUCT<a INT, b STRING>"),
                    # Nested ARRAY
                    "col_array_nested": exp.DataType.build("ARRAY<ARRAY<INT>>"),
                    # Nested MAP (value is ARRAY)
                    "col_map_nested": exp.DataType.build("MAP<STRING,ARRAY<INT>>"),
                    # Nested STRUCT (contains ARRAY and MAP)
                    "col_struct_nested": exp.DataType.build(
                        "STRUCT<id INT, tags ARRAY<STRING>, metadata MAP<STRING,INT>>"
                    ),
                    # ARRAY of STRUCT
                    "col_array_of_struct": exp.DataType.build(
                        "ARRAY<STRUCT<id INT, name STRING>>"
                    ),
                    # Deep nesting: MAP with ARRAY of STRUCT
                    "col_deep_nested": exp.DataType.build(
                        "MAP<STRING,ARRAY<STRUCT<field1 INT, field2 STRING>>>"
                    ),
                },
            )

            # Verify all columns created
            columns = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_complex_types_table' "
                f"ORDER BY ORDINAL_POSITION"
            )
            assert len(columns) == 9, f"Expected 9 columns, got {len(columns)}"

            # Test data insertion with nested types
            # Note: Syntax may vary depending on StarRocks version
            starrocks_adapter.execute(
                f"""
                INSERT INTO {table_name}
                (id, col_array_simple, col_map_simple, col_struct_simple,
                 col_array_nested, col_map_nested, col_struct_nested,
                 col_array_of_struct, col_deep_nested)
                VALUES (
                    1,
                    [1,2,3],
                    map{{'key1':10,'key2':20}},
                    row(100,'simple'),
                    [[1,2],[3,4]],
                    map{{'arr1':[1,2],'arr2':[3,4]}},
                    row(1001, ['tag1','tag2'], map{{'meta1':1,'meta2':2}}),
                    [row(1,'Alice'), row(2,'Bob')],
                    map{{'group1':[row(10,'field_a'), row(20,'field_b')]}}
                )
                """
            )

            # Verify insertion
            count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {table_name}")
            assert count[0] == 1, "Data insertion with complex nested types failed"

            # Verify data retrieval for simple types
            result = starrocks_adapter.fetchone(
                f"SELECT col_array_simple, col_struct_simple FROM {table_name}"
            )
            assert result is not None, "Failed to retrieve complex type data"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)


class TestEndToEndModelParsing:
    """
    End-to-End Model Parsing Integration Tests

    These tests verify the BASIC and COMPLETE pipeline from MODEL definition to SQL execution.
    And will cover some important edge cases, to know whether the whole process can work:

    MODEL Definition (String)
        ↓
    d.parse() + load_sql_based_model()
        ↓
    Model Object (with physical_properties, partitioned_by_, clustered_by, etc.)
        ↓
    adapter.create_table(
        partitioned_by=model.partitioned_by_,     # MODEL-level parameter
        clustered_by=model.clustered_by,          # MODEL-level parameter
        table_properties=model.physical_properties # From physical_properties block
    )
        ↓
    SQL Generation
        ↓
    Execute on Real StarRocks
        ↓
    Verify via SHOW CREATE TABLE (with ACTUAL column names)

    This ensures that the parameter forms passed to create_table() match
    what SQLMesh actually produces when parsing a .sql model file.

    Test Categories:
    ================

    1. Physical Properties Tests (properties inside physical_properties block):
    2. Model-Level Parameter Tests (parameters at MODEL level, not in physical_properties):

    Property Test Matrix (End-to-End):
    +------------------+----------------------------------------+----------------------------------------+
    | Property         | MODEL Syntax                           | Expected DDL                           |
    +------------------+----------------------------------------+----------------------------------------+
    | primary_key      | primary_key = (order_id, event_date)   | PRIMARY KEY (order_id, event_date)     |
    | duplicate_key    | duplicate_key = (id, name)             | DUPLICATE KEY (id, name)               |
    | partitioned_by   | partitioned_by (event_date)            | PARTITION BY RANGE (event_date)        |
    | distributed_by   | distributed_by = (kind='HASH', ...)    | DISTRIBUTED BY HASH (id) BUCKETS N     |
    | clustered_by     | clustered_by (order_id, region)        | ORDER BY (order_id, region)            |
    | order_by         | order_by = (dt, region)                | ORDER BY (dt, region)                  |
    | replication_num  | replication_num = '1'                  | PROPERTIES ('replication_num'='1')     |
    +------------------+----------------------------------------+----------------------------------------+
    """

    def _parse_model_and_get_all_params(self, model_sql: str) -> t.Dict[str, t.Any]:
        """
        Helper: Parse MODEL definition and extract ALL parameters.

        This method returns a dictionary containing ALL parameters that would be passed
        to adapter.create_table(), matching what SQLMesh actually does when processing
        a model file. This ensures tests verify the real parameter forms, not hand-crafted ones.

        Returns:
            Dict containing:
            - physical_properties: Dict[str, exp.Expression] from MODEL's physical_properties
            - partitioned_by: List[exp.Expression] from MODEL's partitioned_by parameter
            - clustered_by: List[exp.Expression] from MODEL's clustered_by parameter
            - target_columns_to_types: Dict[str, exp.DataType] from MODEL's columns or query
            - table_description: Optional[str] from MODEL's description
            - storage_format: Optional[str] from MODEL's storage_format
        """
        expressions = d.parse(model_sql, default_dialect="starrocks")
        model = load_sql_based_model(expressions, dialect="starrocks")
        logger.debug(f"model params: {model}")

        return {
            "partitioned_by": model.partitioned_by_,
            "clustered_by": model.clustered_by,
            "target_columns_to_types": model.columns_to_types or {},
            "table_description": model.description,
            "storage_format": model.storage_format,
            "table_properties": model.physical_properties,
        }

    # ========================================
    # Case 1: Model Parameters (test_design.md Case 1)
    # Covers: partitioned_by (multi-expr with function), clustered_by (multi-column)
    # ========================================

    def test_e2e_model_parameters(self, starrocks_adapter: StarRocksEngineAdapter):
        """
        Test Case 1: Model-level parameters (partitioned_by + clustered_by).

        Covers: partitioned_by (multi-expr with function), clustered_by (multi-column)
        """
        db_name = "sr_e2e_model_params_db"
        table_name = f"{db_name}.sr_model_params_table"

        model_sql = """
        MODEL (
            name test.model_parameters,
            kind FULL,
            columns (
                ts BIGINT,
                region VARCHAR(50),
                order_id BIGINT,
                customer_id INT
            ),
            partitioned_by (from_unixtime(ts), region),  -- Multi-expr with function
            clustered_by (order_id, customer_id)         -- Multi-column
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 1 DDL:\n{ddl}")

            # Precise assertions: verify PARTITION BY RANGE with actual columns
            import re

            assert "PARTITION BY " in ddl
            # Note: PARTITION BY may contain function expressions like from_unixtime(ts)
            # We verify the clause exists and contains expected patterns
            part_match = re.search(r"PARTITION BY \s*\(([^)]+)\)", ddl)
            assert part_match, "PARTITION BY clause not found"
            part_cols = part_match.group(1)
            # Verify function expression and column references
            assert (
                # "from_unixtime" in part_cols or "ts" in part_cols
                "__generated_partition_column_" in part_cols
                and "region" in part_cols
            ), f"Expected partition expression with generated column/region, got {part_cols}"

            # Verify ORDER BY from clustered_by
            order_match = re.search(r"ORDER BY\s*\(([^)]+)\)", ddl)
            assert order_match, "ORDER BY clause not found"
            order_cols = order_match.group(1)
            assert (
                "order_id" in order_cols and "customer_id" in order_cols
            ), f"Expected ORDER BY (order_id, customer_id), got {order_cols}"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 2: Physical Properties Core (test_design.md Case 2)
    # Covers: primary_key (tuple), distributed_by (string multi-col), order_by (tuple), generic props
    # ========================================

    def test_e2e_physical_properties_core(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """
        Test Case 2: Core physical_properties.

        Covers: primary_key (tuple), distributed_by (string multi-col), order_by (tuple), generic props
        """
        db_name = "sr_e2e_core_props_db"
        table_name = f"{db_name}.sr_core_props_table"

        model_sql = """
        MODEL (
            name test.physical_props_core,
            kind FULL,
            dialect starrocks,
            columns (
                order_id BIGINT,
                event_date DATE,
                customer_id INT,
                region VARCHAR(50),
                amount DECIMAL(18,2)
            ),
            physical_properties (
                primary_key = (order_id, event_date, customer_id, region),
                distributed_by = "HASH(customer_id, region) BUCKETS 16",
                order_by = (order_id, region),
                -- clustered_by = (order_id, region),  -- also OK
                -- replication_num = '1',
                bucket_size = '12345678',
                enable_persistent_index = 'true'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 2 DDL:\n{ddl}")

            # Precise assertions
            import re

            # Verify PRIMARY KEY with exact columns
            pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            assert "order_id" in pk_match.group(1) and "event_date" in pk_match.group(1)

            # Verify DISTRIBUTED BY HASH with exact columns
            dist_match = re.search(r"DISTRIBUTED BY HASH\s*\(([^)]+)\)", ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            dist_cols = dist_match.group(1)
            assert (
                "customer_id" in dist_cols and "region" in dist_cols
            ), f"Expected HASH(customer_id, region), got HASH({dist_cols})"
            assert "BUCKETS 16" in ddl

            # Verify ORDER BY
            order_match = re.search(r"ORDER BY\s*\(([^)]+)\)", ddl)
            assert order_match, "ORDER BY clause not found"
            assert "order_id" in order_match.group(1) and "region" in order_match.group(
                1
            )

            # assert "replication_num" not in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 3: String No-Paren Auto-Wrap (test_design.md Case 3)
    # Covers: primary_key = "id, dt" auto-conversion
    # ========================================

    def test_e2e_string_no_paren_auto_wrap(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """
        Test Case 3: String form without parentheses auto-wrap.

        Covers: primary_key = "id, dt" auto-conversion (multi-column string)
        """
        db_name = "sr_e2e_auto_wrap_db"
        table_name = f"{db_name}.sr_auto_wrap_table"

        model_sql = """
        MODEL (
            name test.string_no_paren,
            kind FULL,
            dialect starrocks,
            columns (
                order_id BIGINT,
                event_date DATE
            ),
            physical_properties (
                primary_key = "order_id, event_date",  -- No parentheses, auto-wrapped
                distributed_by = 'HASH(order_id) BUCKETS 10',
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 3 DDL:\n{ddl}")

            # Precise assertion: verify exact PRIMARY KEY columns
            import re

            pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            pk_clause = pk_match.group(1)
            assert (
                "order_id" in pk_clause and "event_date" in pk_clause
            ), f"Expected both order_id and event_date in PRIMARY KEY, got {pk_clause}"

            # Verify distributed_by with exact columns
            dist_match = re.search(r"DISTRIBUTED BY HASH\s*\(([^)]+)\)", ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            assert "order_id" in dist_match.group(
                1
            ), f"Expected HASH(order_id), got HASH({dist_match.group(1)})"
            assert "BUCKETS 10" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 4: Structured Distribution (test_design.md Case 4)
    # Covers: kind=HASH (unquoted), kind=RANDOM
    # ========================================

    def test_e2e_distribution_structured_hash(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """Test Case 4A: Structured HASH distribution with unquoted kind."""
        db_name = "sr_e2e_dist_hash_db"
        table_name = f"{db_name}.sr_dist_hash_table"

        model_sql = """
        MODEL (
            name test.dist_hash_structured,
            kind FULL,
            dialect starrocks,
            columns (
                customer_id INT,
                region VARCHAR(50)
            ),
            physical_properties (
                distributed_by = (kind=HASH, expressions=(customer_id, region), buckets=16),
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 4A DDL:\n{ddl}")

            # Precise assertions
            import re

            assert "DISTRIBUTED BY HASH" in ddl
            dist_match = re.search(r"DISTRIBUTED BY HASH\s*\(([^)]+)\)", ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            assert "customer_id" in dist_match.group(
                1
            ) and "region" in dist_match.group(1)
            assert "BUCKETS 16" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_e2e_distribution_structured_random(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """Test Case 4B: Structured RANDOM distribution."""
        db_name = "sr_e2e_dist_random_db"
        table_name = f"{db_name}.sr_dist_random_table"

        model_sql = """
        MODEL (
            name test.dist_random_structured,
            kind FULL,
            dialect starrocks,
            columns (
                log_id BIGINT,
                event_time DATETIME,
                message VARCHAR(500)
            ),
            physical_properties (
                distributed_by = (kind=RANDOM, buckets=10),
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 4B DDL:\n{ddl}")

            assert "DISTRIBUTED BY RANDOM" in ddl
            assert "BUCKETS 10" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 5: Partition with RANGE (test_design.md Case 5)
    # Covers: partitioned_by RANGE, partitions tuple
    # ========================================

    def test_e2e_partition_range(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test Case 5: RANGE partition with multiple partition definitions."""
        db_name = "sr_e2e_part_range_db"
        table_name = f"{db_name}.sr_part_range_table"

        model_sql = """
        MODEL (
            name test.partition_range,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                year smallint,
                month smallint
            ),
            physical_properties (
                primary_key = (id, year, month),
                partition_by = RANGE(year, month),
                partitions = (
                    'PARTITION p202401 VALUES LESS THAN ("2024", "02")',
                    'PARTITION p202402 VALUES LESS THAN ("2024", "03")',
                    'PARTITION p202403 VALUES LESS THAN ("2024", "04")'
                ),
                distributed_by = 'HASH(id) BUCKETS 10',
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 5 DDL:\n{ddl}")

            # Precise assertions
            import re

            assert "PARTITION BY RANGE" in ddl
            # Verify partition columns
            part_match = re.search(r"PARTITION BY RANGE\s*\(([^)]+)\)", ddl)
            assert part_match, "PARTITION BY RANGE clause not found"
            assert "year" in part_match.group(1) and "month" in part_match.group(1)
            # Verify partition definitions
            assert "p202401" in ddl and "p202402" in ddl and "p202403" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 6: Partition with LIST (test_design.md Case 6)
    # Covers: LIST partition with partitions values
    # ========================================

    def test_e2e_partition_list(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test Case 6: LIST partition."""
        db_name = "sr_e2e_part_list_db"
        table_name = f"{db_name}.sr_part_list_table"

        model_sql = """
        MODEL (
            name test.partition_list,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                region VARCHAR(20)
            ),
            physical_properties (
                primary_key = (id, region),
                partition_by = LIST(region),  -- can't use partitioned_by
                partitions = (
                    'PARTITION p_cn VALUES IN ("cn", "tw", "hk")',
                    'PARTITION p_us VALUES IN ("us", "ca")'
                ),
                distributed_by = 'HASH(id) BUCKETS 8',
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 6 DDL:\n{ddl}")

            # Precise assertions
            import re

            assert "PARTITION BY LIST" in ddl
            # Verify partition column
            part_match = re.search(r"PARTITION BY LIST\s*\(([^)]+)\)", ddl)
            assert part_match, "PARTITION BY LIST clause not found"
            assert "region" in part_match.group(1)
            # Verify partition definitions
            assert "p_cn" in ddl and "p_us" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 7: Other Key Types (test_design.md Case 7)
    # Covers: duplicate_key, unique_key, aggregate_key
    # ========================================

    def test_e2e_key_type_duplicate(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test Case 7A: DUPLICATE KEY."""
        db_name = "sr_e2e_dup_key_db"
        table_name = f"{db_name}.sr_dup_key_table"

        model_sql = """
        MODEL (
            name test.duplicate_key_model,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                dt DATE
            ),
            physical_properties (
                duplicate_key = (id, dt),
                distributed_by = 'HASH(id) BUCKETS 10',
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 7A DDL:\n{ddl}")

            # Verify DUPLICATE KEY with exact columns
            import re

            dup_match = re.search(r"DUPLICATE KEY\s*\(([^)]+)\)", ddl)
            assert dup_match, "DUPLICATE KEY clause not found"
            assert "id" in dup_match.group(1) and "dt" in dup_match.group(
                1
            ), f"Expected DUPLICATE KEY(id, dt), got DUPLICATE KEY({dup_match.group(1)})"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_e2e_key_type_unique(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test Case 7B: UNIQUE KEY."""
        db_name = "sr_e2e_uniq_key_db"
        table_name = f"{db_name}.sr_uniq_key_table"

        model_sql = """
        MODEL (
            name test.unique_key_model,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                dt DATE
            ),
            physical_properties (
                unique_key = (id, dt),
                distributed_by = 'HASH(id) BUCKETS 10',
                replication_num = '1'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 7B DDL:\n{ddl}")

            assert "UNIQUE KEY" in ddl, "UNIQUE KEY missing"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_e2e_key_type_aggregate(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test Case 7C: AGGREGATE KEY - should raise exception (unsupported)."""
        db_name = "sr_e2e_agg_key_db"
        table_name = f"{db_name}.sr_agg_key_table"

        model_sql = """
        MODEL (
            name test.aggregate_key_model,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                dt DATE
            ),
            physical_properties (
                aggregate_key = (id, dt),
                distributed_by = 'HASH(id) BUCKETS 10',
                replication_num = '1'
            )
        );
        SELECT *
        """

        from sqlmesh.utils.errors import SQLMeshError
        import pytest

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)

            # Expect SQLMeshError to be raised for unsupported AGGREGATE KEY
            with pytest.raises(SQLMeshError, match="AGGREGATE KEY.*not supported"):
                starrocks_adapter.create_table(table_name, **params)

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Final: Comprehensive Test (all properties combined)
    # ========================================

    def test_e2e_comprehensive(self, starrocks_adapter: StarRocksEngineAdapter):
        """Final: Comprehensive test with ALL property types combined."""
        db_name = "sr_e2e_comprehensive_db"
        table_name = f"{db_name}.sr_comprehensive_table"

        model_sql = """
        MODEL (
            name test.comprehensive_model,
            kind FULL,
            dialect starrocks,
            description 'Comprehensive test table with all properties',
            columns (
                order_id BIGINT,
                event_date DATE,
                customer_id INT,
                amount DECIMAL(18,2),
                status VARCHAR(20)
            ),
            partitioned_by (event_date),
            clustered_by (order_id, event_date),
            physical_properties (
                primary_key = (order_id, event_date),
                distributed_by = (kind=HASH, expressions=order_id, buckets=8),
                replication_num = '1',
                storage_medium = 'HDD'
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Comprehensive DDL:\n{ddl}")

            # Precise assertions for all major clauses
            import re

            # Verify PRIMARY KEY
            pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            assert "order_id" in pk_match.group(1) and "event_date" in pk_match.group(1)

            # Verify PARTITION BY
            assert "PARTITION BY" in ddl
            # Verify exact partition column
            part_match = re.search(r"PARTITION BY[^(]*\(([^)]+)\)", ddl)
            assert part_match, "PARTITION BY clause not found"
            part_cols = part_match.group(1)
            assert (
                "event_date" in part_cols
            ), f"Expected event_date in PARTITION BY, got {part_cols}"

            # Verify DISTRIBUTED BY
            assert "DISTRIBUTED BY HASH" in ddl
            assert "BUCKETS 8" in ddl

            # Verify ORDER BY
            order_match = re.search(r"ORDER BY\s*\(([^)]+)\)", ddl)
            assert order_match, "ORDER BY clause not found"
            assert "order_id" in order_match.group(
                1
            ) and "event_date" in order_match.group(1)

            # Verify PROPERTIES
            assert "replication_num" in ddl

            # Functional test
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} "
                f"(order_id, event_date, customer_id, amount, status) "
                f"VALUES (1001, '2024-01-15', 100, 1234.56, 'completed')"
            )

            result = starrocks_adapter.fetchone(
                f"SELECT order_id, customer_id FROM {table_name} WHERE order_id = 1001"
            )
            assert result is not None, "INSERT/SELECT failed"
            assert result[0] == 1001, "order_id mismatch"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Quote Character Handling Test
    # Tests single quotes vs double quotes in MODEL parsing
    # ========================================

    def test_e2e_quote_character_handling(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """
        Test Case: Quote Character Handling (Single vs Double Quotes).

        This test verifies that MODEL parsing correctly handles different quote types:
        - Single quotes 'value' → Literal(is_string=True) ✓
        - Double quotes "value" → Column(quoted=True) (parser quirk, but we handle it) ✓
        - Bare identifiers → proper parsing

        We test this by using different quote forms in MODEL physical_properties
        and verifying that the final DDL is correct.

        Quote Behavior:
        ===============
        In MySQL/StarRocks:
        - Backtick ` : identifier quote
        - Single quote ': string literal
        - Double quote ": string literal (default) OR identifier (ANSI_QUOTES mode)

        In SQLMesh MODEL parsing:
        - Single quotes 'value' → exp.Literal (correct)
        - Double quotes "value" → exp.Column(quoted=True) (inconsistent with SQL, but handled)

        This test ensures our workaround in ensure_parenthesized() works correctly.
        """
        db_name = "sr_e2e_quote_handling_db"
        table_name = f"{db_name}.sr_quote_test_table"

        # Test with different quote forms in MODEL
        model_sql = """
        MODEL (
            name test.quote_handling_model,
            kind FULL,
            dialect starrocks,
            columns (
                id BIGINT,
                dt DATE,
                region VARCHAR(50),
                customer_id INT
            ),
            physical_properties (
                -- Single quotes (correct way) - parses to Literal
                primary_key = 'id, dt, region',

                partition_by = "date_trunc('day', dt), region",

                -- Double quotes (parser quirk) - parses to Column(quoted=True)
                -- But our ensure_parenthesized handles this
                order_by = "id, region",

                -- Structured form with single-quoted string
                distributed_by = 'HASH(id) BUCKETS 8',

                -- Generic properties with single quotes
                replication_num = '1',
                -- storage_medium = "HDD"  -- not valid in shared-data cluster
            )
        );
        SELECT *
        """

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # Parse MODEL and extract parameters (this is where quote handling happens)
            params = self._parse_model_and_get_all_params(model_sql)

            # Log parsed parameters for debugging
            logger.info(f"Parsed physical_properties: {params['table_properties']}")
            for key, value in params["table_properties"].items():
                logger.info(f"  {key}: {type(value).__name__} = {value}")

            # Create table with parsed parameters
            starrocks_adapter.create_table(table_name, **params)

            # Verify via SHOW CREATE TABLE
            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Quote Handling Test DDL:\n{ddl}")

            # Precise assertions
            import re

            # 1. Verify PRIMARY KEY (from single-quoted string 'id, dt')
            pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            pk_cols = pk_match.group(1)
            assert "id" in pk_cols and "dt" in pk_cols, (
                f"Expected PRIMARY KEY (id, dt), got {pk_cols}. "
                f"Single-quoted string 'id, dt' was not correctly parsed!"
            )

            # 2. Verify ORDER BY (from double-quoted string \"id, region\")
            # This tests our Column(quoted=True) workaround
            order_match = re.search(r"ORDER BY\s*\(([^)]+)\)", ddl)
            assert order_match, "ORDER BY clause not found"
            order_cols = order_match.group(1)
            assert "id" in order_cols and "region" in order_cols, (
                f"Expected ORDER BY (id, region), got {order_cols}. "
                f'Double-quoted string "id, region" was not correctly handled!'
            )

            # 3. Verify DISTRIBUTED BY (from single-quoted string)
            assert "DISTRIBUTED BY HASH" in ddl, "DISTRIBUTED BY clause not found"
            assert "customer_id" in ddl, "customer_id not found in DISTRIBUTED BY"
            assert "BUCKETS 8" in ddl, "BUCKETS not found in DISTRIBUTED BY"

            # 4. Verify PROPERTIES (generic properties with single quotes)
            assert "replication_num" in ddl, "replication_num not found in PROPERTIES"
            # assert "storage_medium" in ddl or "HDD" in ddl, "storage_medium not found in PROPERTIES"

            # Functional test: Verify table actually works
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} "
                f"(id, dt, region, customer_id) "
                f"VALUES (100, '2024-01-01', 'US', 1001)"
            )

            result = starrocks_adapter.fetchone(
                f"SELECT id, region, customer_id FROM {table_name} WHERE id = 100"
            )
            assert result is not None, "INSERT/SELECT failed"
            assert result == (100, "US", 1001), f"Data mismatch: {result}"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)


# ==================== StarRocks Native SQL Capabilities ====================


class TestStarRocksAbility:
    """
    Test StarRocks native SQL capabilities and limitations.

    This test class validates StarRocks database features by executing
    raw SQL statements directly, without going through SQLMesh abstraction layers.

    Purpose:
    - Document which SQL features are supported
    - Verify expected failures for unsupported operations
    - Guide adapter implementation decisions

    Note: Tests marked with @pytest.mark.xfail are EXPECTED to fail.
    """

    @pytest.fixture(scope="class")
    def test_tables(
        self, starrocks_adapter: StarRocksEngineAdapter
    ) -> t.Dict[str, str]:
        """
        Pre-create tables of different types for testing.

        Returns:
            Dict mapping table type to fully qualified table name
        """
        db_name = "sr_ability_test"
        starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

        tables = {}

        # 1. PRIMARY KEY table
        # Note: StarRocks PRIMARY KEY tables support complex DELETE operations (BETWEEN, subqueries, etc.)
        pk_table = f"{db_name}.pk_table"
        starrocks_adapter.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {pk_table} (
                id INT,
                dt DATE,
                name STRING,
                status STRING
            ) PRIMARY KEY (id, dt)
            DISTRIBUTED BY HASH(id) BUCKETS 10
        """
        )
        # Verify table creation
        result = starrocks_adapter.fetchone(
            f"SELECT COUNT(*) FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'pk_table'"
        )
        assert result[0] == 1, f"PRIMARY KEY table {pk_table} creation failed"
        tables["primary_key"] = pk_table

        # 2. DUPLICATE KEY table
        dup_table = f"{db_name}.dup_table"
        starrocks_adapter.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {dup_table} (
                id INT,
                dt DATE,
                name STRING,
                status STRING
            ) DUPLICATE KEY (id, dt)
            DISTRIBUTED BY HASH(id) BUCKETS 10
        """
        )
        # Verify table creation
        result = starrocks_adapter.fetchone(
            f"SELECT COUNT(*) FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'dup_table'"
        )
        assert result[0] == 1, f"DUPLICATE KEY table {dup_table} creation failed"
        tables["duplicate_key"] = dup_table

        # 3. UNIQUE KEY table
        unique_table = f"{db_name}.unique_table"
        starrocks_adapter.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {unique_table} (
                id INT,
                dt DATE,
                name STRING,
                status STRING
            ) UNIQUE KEY (id, dt)
            DISTRIBUTED BY HASH(id) BUCKETS 10
        """
        )
        # Verify table creation
        result = starrocks_adapter.fetchone(
            f"SELECT COUNT(*) FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'unique_table'"
        )
        assert result[0] == 1, f"UNIQUE KEY table {unique_table} creation failed"
        tables["unique_key"] = unique_table

        yield tables

        # Cleanup
        starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ==================== Schema Operations ====================

    @pytest.mark.parametrize("sql_keyword", ["SCHEMA", "DATABASE"])
    def test_create_drop_keyword_support(
        self, starrocks_adapter: StarRocksEngineAdapter, sql_keyword: str
    ):
        """
        Test both CREATE SCHEMA and CREATE DATABASE syntax.

        Expected: Both keywords should work (they are synonyms in StarRocks)
        """
        test_name = f"sr_ability_{sql_keyword.lower()}"

        try:
            # CREATE
            starrocks_adapter.execute(f"CREATE {sql_keyword} IF NOT EXISTS {test_name}")
            result = starrocks_adapter.fetchone(
                f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{test_name}'"
            )
            assert result is not None, f"CREATE {sql_keyword} failed"

            # DROP
            starrocks_adapter.execute(f"DROP {sql_keyword} IF EXISTS {test_name}")
            result = starrocks_adapter.fetchone(
                f"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '{test_name}'"
            )
            assert result is None, f"DROP {sql_keyword} failed"

        finally:
            starrocks_adapter.execute(f"DROP {sql_keyword} IF EXISTS {test_name}")

    # ==================== DELETE Operations - Success Cases ====================

    @pytest.mark.parametrize(
        "table_type,delete_clause,expected_remaining",
        [
            # PRIMARY KEY table - full support
            ("primary_key", "WHERE id = 1", 2),
            ("primary_key", "WHERE dt BETWEEN '2024-01-01' AND '2024-06-30'", 1),
            (
                "primary_key",
                "WHERE id IN (SELECT id FROM {table} WHERE status = 'deleted')",
                2,
            ),
            ("primary_key", "WHERE TRUE", 0),
            # PRIMARY KEY with USING (JOIN delete)
            (
                "primary_key",
                "USING {table} t2 WHERE {table}.id = t2.id AND t2.status = 'deleted'",
                2,
            ),
            # DUPLICATE/UNIQUE KEY - only simple WHERE
            ("duplicate_key", "WHERE id = 1", 2),
            ("unique_key", "WHERE id = 1", 2),
        ],
        ids=[
            "pk_simple_where",
            "pk_between",
            "pk_subquery",
            "pk_where_true",
            "pk_using_join",
            "dup_simple_where",
            "unique_simple_where",
        ],
    )
    def test_delete_supported_syntax(
        self,
        starrocks_adapter: StarRocksEngineAdapter,
        test_tables: t.Dict[str, str],
        table_type: str,
        delete_clause: str,
        expected_remaining: int,
    ):
        """
        Test DELETE operations that should succeed.

        Expected: DELETE succeeds and leaves expected number of rows
        """
        table_name = test_tables[table_type]

        # Prepare test data for this specific test (better isolation)
        # All tables have the same column structure: (id, dt, name, status)
        test_data = """
            (1, '2024-01-15', 'Alice', 'active'),
            (2, '2024-06-10', 'Bob', 'deleted'),
            (3, '2024-12-05', 'Charlie', 'active')
        """
        starrocks_adapter.execute(f"TRUNCATE TABLE {table_name}")
        starrocks_adapter.execute(f"INSERT INTO {table_name} VALUES {test_data}")

        # Format delete clause (for subquery/using with table reference)
        delete_sql = (
            f"DELETE FROM {table_name} {delete_clause.format(table=table_name)}"
        )

        # Debug: Log the SQL before execution
        logger.info(f"Executing DELETE SQL: {delete_sql}")

        # Execute delete
        starrocks_adapter.execute(delete_sql)

        # Verify result
        count = starrocks_adapter.fetchone(f"SELECT COUNT(*) FROM {table_name}")[0]
        logger.info(
            f"After DELETE: {count} rows remaining (expected {expected_remaining})"
        )
        assert (
            count == expected_remaining
        ), f"Expected {expected_remaining} rows, got {count} for {table_type} with {delete_clause}"

    # ==================== DELETE Operations - Failure Cases ====================

    syntax_error = "not supported|syntax error|getting analyzing error"

    @pytest.mark.parametrize(
        "table_type,delete_clause,error_pattern",
        [
            # DUPLICATE KEY - unsupported syntax
            (
                "duplicate_key",
                "WHERE dt BETWEEN '2024-01-01' AND '2024-12-31'",
                syntax_error,
            ),
            (
                "duplicate_key",
                "WHERE id IN (SELECT id FROM {table} WHERE status = 'deleted')",
                syntax_error,
            ),
            ("duplicate_key", "WHERE TRUE", syntax_error),
            # UNIQUE KEY - unsupported syntax
            (
                "unique_key",
                "WHERE dt BETWEEN '2024-01-01' AND '2024-12-31'",
                syntax_error,
            ),
            ("unique_key", "WHERE id IN (SELECT id FROM {table})", syntax_error),
        ],
        ids=[
            "dup_between_unsupported",
            "dup_subquery_unsupported",
            "dup_where_true_unsupported",
            "unique_between_unsupported",
            "unique_subquery_unsupported",
        ],
    )
    def test_delete_unsupported_syntax(
        self,
        starrocks_adapter: StarRocksEngineAdapter,
        test_tables: t.Dict[str, str],
        table_type: str,
        delete_clause: str,
        error_pattern: str,
    ):
        """
        Test DELETE operations that should fail on non-PRIMARY KEY tables.

        Expected: DELETE fails with specific error message.
        """
        table_name = test_tables[table_type]
        delete_sql = (
            f"DELETE FROM {table_name} {delete_clause.format(table=table_name)}"
        )

        # This should raise an exception
        with pytest.raises(Exception) as exc_info:
            starrocks_adapter.execute(delete_sql)

        # Verify error message matches expected pattern
        import re

        error_msg = str(exc_info.value).lower()
        assert re.search(
            error_pattern, error_msg
        ), f"Expected error pattern '{error_pattern}', got: {exc_info.value}"

    # ==================== COMMENT Syntax Tests ====================

    @pytest.mark.parametrize(
        "comment_type,sql_template",
        [
            # Table comment variants
            ("table_standard", "ALTER TABLE {table} COMMENT = '{comment}'"),
            # Failed without `=`
            # ("table_standard", "ALTER TABLE {table} COMMENT '{comment}'"),
            # No MODIFY keyworkd
            # ("table_modify", 'ALTER TABLE {table} MODIFY COMMENT "{comment}"'),
            # # Column comment variants
            (
                "column_no_type",
                "ALTER TABLE {table} MODIFY COLUMN {column} COMMENT '{comment}'",
            ),
            # it will take some time to change the column type
            # ("column_with_type", "ALTER TABLE {table} MODIFY COLUMN {column} BIGINT COMMENT '{comment}'"),
        ],
        ids=[
            "table_comment_standard",
            # "table_comment_standard_without_equal",  # FAIL
            # "table_comment_modify",  # FAIL
            "column_comment_no_type",
            # "column_comment_with_type",
        ],
    )
    def test_comment_syntax_variants(
        self,
        starrocks_adapter: StarRocksEngineAdapter,
        comment_type: str,
        sql_template: str,
    ):
        """
        Test different COMMENT syntax variations to determine StarRocks support.

        Purpose: Guide whether we need to override comment methods in adapter
        """
        db_name = "sr_ability_comment"
        table_name = f"{db_name}.test_comment"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT,
                    col1 INT
                )
                DUPLICATE KEY (id)  -- key columns can't be changed.
                DISTRIBUTED BY HASH(id) BUCKETS 10
            """
            )

            # Generate SQL based on template
            if "table" in comment_type:
                sql = sql_template.format(
                    table=table_name, comment=f"test {comment_type}"
                )
            else:  # column
                sql = sql_template.format(
                    table=table_name, column="col1", comment=f"test {comment_type}"
                )

            # Try to execute
            try:
                starrocks_adapter.execute(sql)

                # Verify comment was set
                if "table" in comment_type:
                    result = starrocks_adapter.fetchone(
                        f"SELECT TABLE_COMMENT FROM information_schema.TABLES "
                        f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_comment'"
                    )[0]
                    assert (
                        f"test {comment_type}" in result
                    ), f"Comment not set correctly for {comment_type}"
                else:  # column
                    result_row = starrocks_adapter.fetchone(
                        f"SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
                        f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_comment' "
                        f"AND COLUMN_NAME = 'col1'"
                    )
                    logger.info(f"Column comment: {result_row}")
                    result = result_row[1]
                    assert (
                        f"test {comment_type}" in result
                    ), f"Comment not set correctly for {comment_type}"

                logger.info(f"✅ {comment_type}: SUPPORTED")

            except Exception as e:
                logger.warning(f"❌ {comment_type}: NOT SUPPORTED - {e}")
                # Re-raise for test failure
                raise

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ==================== Quote Type Tests ====================

    @pytest.mark.parametrize(
        "quote_type,comment_value",
        [
            ("single", "single quotes"),
            ("double", "double quotes"),
            ("escaped_single", "It\\'s a test"),
            ("escaped_double", 'Say \\"hello\\"'),
        ],
        ids=["single_quotes", "double_quotes", "escaped_single", "escaped_double"],
    )
    def test_comment_quote_types(
        self,
        starrocks_adapter: StarRocksEngineAdapter,
        quote_type: str,
        comment_value: str,
    ):
        """
        Test different quote types in COMMENT clauses.

        Purpose: Determine which quote types StarRocks accepts
        """
        db_name = "sr_ability_quotes"
        table_name = f"{db_name}.test_quotes"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.execute(
                f"""
                CREATE TABLE {table_name} (id INT)
                DISTRIBUTED BY HASH(id) BUCKETS 10
            """
            )

            # Build SQL with appropriate quotes
            if "single" in quote_type:
                sql = f"ALTER TABLE {table_name} COMMENT = '{comment_value}'"
            else:  # double
                sql = f'ALTER TABLE {table_name} COMMENT = "{comment_value}"'

            starrocks_adapter.execute(sql)
            logger.info(f"✅ {quote_type}: SUPPORTED")

        except Exception as e:
            logger.warning(f"❌ {quote_type}: NOT SUPPORTED - {e}")
            raise

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_comment_in_create_table(self, starrocks_adapter: StarRocksEngineAdapter):
        """
        Test COMMENT clauses in CREATE TABLE statement.

        Expected: Verify comments are registered during table creation
        """
        db_name = "sr_ability_create_comment"
        table_name = f"{db_name}.test_create_comment"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # Create table with comments
            starrocks_adapter.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT COMMENT 'id column',
                    name VARCHAR(100) COMMENT 'name column'
                )
                PRIMARY KEY (id)
                COMMENT 'test table'
                DISTRIBUTED BY HASH(id) BUCKETS 10
            """
            )

            # Verify table comment
            table_comment = starrocks_adapter.fetchone(
                f"SELECT TABLE_COMMENT FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_create_comment'"
            )[0]
            assert (
                table_comment == "test table"
            ), f"Table comment mismatch: {table_comment}"

            # Verify column comments
            column_comments = {}
            results = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_create_comment'"
            )
            for col_name, col_comment in results:
                if col_comment:  # Skip empty comments
                    column_comments[col_name] = col_comment

            assert (
                column_comments.get("id") == "id column"
            ), f"Column comment mismatch: {column_comments}"
            assert (
                column_comments.get("name") == "name column"
            ), f"Column comment mismatch: {column_comments}"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)


class TestCommentMethods:
    """
    Test _build_create_comment_table_exp and _build_create_comment_column_exp methods.

    These methods are used to generate ALTER TABLE SQL for modifying comments.
    Although StarRocks uses COMMENT_CREATION_TABLE = IN_SCHEMA_DEF_CTAS (comments
    are included in CREATE TABLE), these methods may be used for:
    - Modifying existing table comments
    - View comments (depending on COMMENT_CREATION_VIEW)
    - Future ALTER TABLE support
    """

    def test_build_create_comment_table_exp(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """
        Test _build_create_comment_table_exp generates correct ALTER TABLE COMMENT SQL.

        Verifies:
        1. Method generates correct SQL syntax
        2. SQL can be executed successfully
        3. Comment is actually updated in database
        """
        db_name = "sr_test_comment_table"
        table_name = f"{db_name}.test_table"

        try:
            # Setup: Create schema and table
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT,
                    name VARCHAR(100)
                )
                PRIMARY KEY (id)
                COMMENT 'initial comment'
                DISTRIBUTED BY HASH(id) BUCKETS 10
            """
            )

            # Test: Use _build_create_comment_table_exp to generate SQL
            table_expr = exp.to_table(table_name)
            new_comment = "Updated table comment via method"
            comment_sql = starrocks_adapter._build_create_comment_table_exp(
                table=table_expr, table_comment=new_comment, table_kind="TABLE"
            )

            # Verify: SQL format is correct
            assert "ALTER TABLE" in comment_sql, f"Invalid SQL format: {comment_sql}"
            assert (
                "COMMENT =" in comment_sql
            ), f"Missing COMMENT = in SQL: {comment_sql}"
            assert new_comment in comment_sql, f"Comment not in SQL: {comment_sql}"

            # Execute the generated SQL
            starrocks_adapter.execute(comment_sql)

            # Verify: Comment was actually updated
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_COMMENT FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_table'"
            )
            assert result is not None, "Table not found after comment update"
            assert (
                result[0] == new_comment
            ), f"Comment not updated. Expected: {new_comment}, Got: {result[0]}"

            logger.info("✅ _build_create_comment_table_exp generates valid SQL")

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_build_create_comment_column_exp(
        self, starrocks_adapter: StarRocksEngineAdapter
    ):
        """
        Test _build_create_comment_column_exp generates correct ALTER TABLE MODIFY COLUMN SQL.

        Verifies:
        1. Method generates correct SQL with column type
        2. SQL can be executed successfully
        3. Column comment is actually updated in database
        4. Column type is preserved (not changed)
        """
        db_name = "sr_test_comment_column"
        table_name = f"{db_name}.test_table"

        try:
            # Setup: Create schema and table
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)
            starrocks_adapter.execute(
                f"""
                CREATE TABLE {table_name} (
                    id INT COMMENT 'initial id comment',
                    name VARCHAR(100) COMMENT 'initial name comment',
                    amount DECIMAL(10, 2)
                )
                PRIMARY KEY (id)
                DISTRIBUTED BY HASH(id) BUCKETS 10
            """
            )

            # Test: Use _build_create_comment_column_exp to generate SQL
            table_expr = exp.to_table(table_name)
            new_comment = "Updated column comment via method"
            comment_sql = starrocks_adapter._build_create_comment_column_exp(
                table=table_expr,
                column_name="name",
                column_comment=new_comment,
                table_kind="TABLE",
            )

            # Verify: SQL format is correct
            assert "ALTER TABLE" in comment_sql, f"Invalid SQL format: {comment_sql}"
            assert (
                "MODIFY COLUMN" in comment_sql
            ), f"Missing MODIFY COLUMN in SQL: {comment_sql}"
            assert "COMMENT" in comment_sql, f"Missing COMMENT in SQL: {comment_sql}"
            assert new_comment in comment_sql, f"Comment not in SQL: {comment_sql}"

            # Execute the generated SQL
            starrocks_adapter.execute(comment_sql)

            # Verify: Column comment was actually updated
            result = starrocks_adapter.fetchone(
                f"SELECT COLUMN_TYPE, COLUMN_COMMENT FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'test_table' AND COLUMN_NAME = 'name'"
            )
            assert result is not None, "Column not found after comment update"
            column_type, column_comment = result
            assert (
                column_comment == new_comment
            ), f"Comment not updated. Expected: {new_comment}, Got: {column_comment}"
            assert (
                "varchar(100)" in column_type.lower()
            ), f"Column type changed unexpectedly: {column_type}"

            logger.info(
                "✅ _build_create_comment_column_exp generates valid SQL with correct type"
            )

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)
