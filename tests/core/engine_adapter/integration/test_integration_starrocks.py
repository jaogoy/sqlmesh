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


@pytest.fixture(autouse=True)
def cleanup_test_objects(starrocks_adapter: StarRocksEngineAdapter):
    """
    Clean up test databases before and after each test.

    Uses pattern-based cleanup: all databases starting with 'sr_' prefix
    will be dropped. This ensures:
    1. No orphan databases from failed tests
    2. No manual cleanup needed
    3. Each test starts with a clean state

    Note: All test db_names MUST use 'sr_' prefix for automatic cleanup.
    """
    def cleanup_all_sr_databases():
        """Drop all databases with 'sr_' prefix."""
        try:
            result = starrocks_adapter.fetchall(
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                "WHERE SCHEMA_NAME LIKE 'sr\\_%'"
            )
            for row in result:
                db_name = row[0]
                try:
                    starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)
                    logger.debug(f"Cleaned up database: {db_name}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {db_name}: {e}")
        except Exception as e:
            logger.warning(f"Failed to list databases for cleanup: {e}")

    # Cleanup before test
    cleanup_all_sr_databases()

    yield

    # Cleanup after test
    cleanup_all_sr_databases()


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
            starrocks_adapter.delete_from(
                exp.to_table(table_name),
                "id = 2"
            )
            count = starrocks_adapter.fetchone(
                f"SELECT COUNT(*) FROM {table_name}"
            )
            assert count[0] == 1, "DELETE failed"
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
                    "col_array_of_struct": exp.DataType.build("ARRAY<STRUCT<id INT, name STRING>>"),
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

    def _parse_model_and_get_all_params(
        self,
        model_sql: str
    ) -> t.Dict[str, t.Any]:
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
            assert "PARTITION BY RANGE" in ddl
            # Note: PARTITION BY may contain function expressions like from_unixtime(ts)
            # We verify the clause exists and contains expected patterns
            part_match = re.search(r'PARTITION BY RANGE\s*\(([^)]+)\)', ddl)
            assert part_match, "PARTITION BY RANGE clause not found"
            part_cols = part_match.group(1)
            # Verify function expression and column references
            assert "from_unixtime" in part_cols or "ts" in part_cols, \
                f"Expected partition expression with ts/from_unixtime, got {part_cols}"

            # Verify ORDER BY from clustered_by
            order_match = re.search(r'ORDER BY\s*\(([^)]+)\)', ddl)
            assert order_match, "ORDER BY clause not found"
            order_cols = order_match.group(1)
            assert "order_id" in order_cols and "customer_id" in order_cols, \
                f"Expected ORDER BY (order_id, customer_id), got {order_cols}"

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 2: Physical Properties Core (test_design.md Case 2)
    # Covers: primary_key (tuple), distributed_by (string multi-col), order_by (tuple), generic props
    # ========================================

    def test_e2e_physical_properties_core(self, starrocks_adapter: StarRocksEngineAdapter):
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
                primary_key = (order_id, event_date),
                distributed_by = 'HASH(customer_id, region) BUCKETS 16',
                order_by = (order_id, region),
                replication_num = '1',
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
            pk_match = re.search(r'PRIMARY KEY\s*\(([^)]+)\)', ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            assert "order_id" in pk_match.group(1) and "event_date" in pk_match.group(1)

            # Verify DISTRIBUTED BY HASH with exact columns
            dist_match = re.search(r'DISTRIBUTED BY HASH\s*\(([^)]+)\)', ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            dist_cols = dist_match.group(1)
            assert "customer_id" in dist_cols and "region" in dist_cols, \
                f"Expected HASH(customer_id, region), got HASH({dist_cols})"
            assert "BUCKETS 16" in ddl

            # Verify ORDER BY
            order_match = re.search(r'ORDER BY\s*\(([^)]+)\)', ddl)
            assert order_match, "ORDER BY clause not found"
            assert "order_id" in order_match.group(1) and "region" in order_match.group(1)

            assert "replication_num" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 3: String No-Paren Auto-Wrap (test_design.md Case 3)
    # Covers: primary_key = "id, dt" auto-conversion
    # ========================================

    def test_e2e_string_no_paren_auto_wrap(self, starrocks_adapter: StarRocksEngineAdapter):
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
            pk_match = re.search(r'PRIMARY KEY\s*\(([^)]+)\)', ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            pk_clause = pk_match.group(1)
            assert "order_id" in pk_clause and "event_date" in pk_clause, \
                f"Expected both order_id and event_date in PRIMARY KEY, got {pk_clause}"

            # Verify distributed_by with exact columns
            dist_match = re.search(r'DISTRIBUTED BY HASH\s*\(([^)]+)\)', ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            assert "order_id" in dist_match.group(1), \
                f"Expected HASH(order_id), got HASH({dist_match.group(1)})"
            assert "BUCKETS 10" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    # ========================================
    # Case 4: Structured Distribution (test_design.md Case 4)
    # Covers: kind=HASH (unquoted), kind=RANDOM
    # ========================================

    def test_e2e_distribution_structured_hash(self, starrocks_adapter: StarRocksEngineAdapter):
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
            dist_match = re.search(r'DISTRIBUTED BY HASH\s*\(([^)]+)\)', ddl)
            assert dist_match, "DISTRIBUTED BY HASH clause not found"
            assert "customer_id" in dist_match.group(1) and "region" in dist_match.group(1)
            assert "BUCKETS 16" in ddl

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

    def test_e2e_distribution_structured_random(self, starrocks_adapter: StarRocksEngineAdapter):
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
                year VARCHAR(10),
                month VARCHAR(10)
            ),
            physical_properties (
                primary_key = (id, year, month),
                partitioned_by = RANGE(year, month),
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
            part_match = re.search(r'PARTITION BY RANGE\s*\(([^)]+)\)', ddl)
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
                partitioned_by = LIST(region),
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
            part_match = re.search(r'PARTITION BY LIST\s*\(([^)]+)\)', ddl)
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
            dup_match = re.search(r'DUPLICATE KEY\s*\(([^)]+)\)', ddl)
            assert dup_match, "DUPLICATE KEY clause not found"
            assert "id" in dup_match.group(1) and "dt" in dup_match.group(1), \
                f"Expected DUPLICATE KEY(id, dt), got DUPLICATE KEY({dup_match.group(1)})"

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
        """Test Case 7C: AGGREGATE KEY."""
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

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            params = self._parse_model_and_get_all_params(model_sql)
            starrocks_adapter.create_table(table_name, **params)

            show_create = starrocks_adapter.fetchone(f"SHOW CREATE TABLE {table_name}")
            ddl = show_create[1]
            logger.info(f"Case 7C DDL:\n{ddl}")

            assert "AGGREGATE KEY" in ddl, "AGGREGATE KEY missing"

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
                partitions = (
                    'PARTITION p202401 VALUES LESS THAN ("2024-02-01")',
                    'PARTITION p202402 VALUES LESS THAN ("2024-03-01")'
                ),
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
            pk_match = re.search(r'PRIMARY KEY\s*\(([^)]+)\)', ddl)
            assert pk_match, "PRIMARY KEY clause not found"
            assert "order_id" in pk_match.group(1) and "event_date" in pk_match.group(1)

            # Verify PARTITION BY
            assert "PARTITION BY RANGE" in ddl
            assert "p202401" in ddl

            # Verify DISTRIBUTED BY
            assert "DISTRIBUTED BY HASH" in ddl
            assert "BUCKETS 8" in ddl

            # Verify ORDER BY
            order_match = re.search(r'ORDER BY\s*\(([^)]+)\)', ddl)
            assert order_match, "ORDER BY clause not found"
            assert "order_id" in order_match.group(1) and "event_date" in order_match.group(1)

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
