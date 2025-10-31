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

import os
import typing as t
from functools import partial

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter

# Mark as docker test (can also run against local StarRocks)
# Remove 'docker' marker if you want to run against local instance only
pytestmark = [pytest.mark.starrocks, pytest.mark.docker, pytest.mark.engine]


@pytest.fixture(scope="module")
def starrocks_connection_config() -> t.Dict[str, t.Any]:
    """StarRocks connection configuration from environment variables."""
    return {
        "host": os.getenv("STARROCKS_HOST", "localhost"),
        "port": int(os.getenv("STARROCKS_PORT", "9030")),
        "user": os.getenv("STARROCKS_USER", "root"),
        "password": os.getenv("STARROCKS_PASSWORD", ""),
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
    """Clean up test databases/tables before and after each test."""
    # Cleanup before test - use sr_ prefix to avoid conflicts with other dialects
    test_objects = ["sr_test_db", "sr_basic_db", "sr_comprehensive_db"]
    for obj in test_objects:
        try:
            starrocks_adapter.drop_schema(obj, ignore_if_not_exists=True)
        except:
            pass

    yield

    # Cleanup after test
    for obj in test_objects:
        try:
            starrocks_adapter.drop_schema(obj, ignore_if_not_exists=True)
        except:
            pass


class TestBasicOperations:
    """
    Category 1: Basic Operations

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
    Category 2: Table Features

    Each test method verifies one CREATE TABLE feature.
    Tests cover all attributes: PRIMARY KEY, comments, data types, etc.
    """

    def test_primary_key(self, starrocks_adapter: StarRocksEngineAdapter):
        """Test PRIMARY KEY support (StarRocks native feature)."""
        db_name = "sr_test_pk_db"
        table_name = f"{db_name}.sr_pk_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with PRIMARY KEY
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("BIGINT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
                primary_key=("id",),
            )

            # Verify table exists
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_pk_table'"
            )
            assert result is not None, "CREATE TABLE with PRIMARY KEY failed"

            # Test PRIMARY KEY constraint works (insert duplicate should fail)
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')"
            )
            # TODO: Verify PRIMARY KEY constraint behavior
            # StarRocks PRIMARY KEY allows duplicates but keeps latest version

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

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
        """Test various data types support."""
        db_name = "sr_test_types_db"
        table_name = f"{db_name}.sr_types_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with multiple data types
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "col_bigint": exp.DataType.build("BIGINT"),
                    "col_int": exp.DataType.build("INT"),
                    "col_varchar": exp.DataType.build("VARCHAR(200)"),
                    "col_decimal": exp.DataType.build("DECIMAL(18,2)"),
                    "col_date": exp.DataType.build("DATE"),
                    "col_datetime": exp.DataType.build("DATETIME"),
                    "col_boolean": exp.DataType.build("BOOLEAN"),
                    "col_json": exp.DataType.build("JSON"),
                },
            )

            # Verify all columns created
            columns = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_types_table' "
                f"ORDER BY ORDINAL_POSITION"
            )
            assert len(columns) == 8, "Not all columns created"

            # Test data insertion with various types
            starrocks_adapter.execute(
                f"""
                INSERT INTO {table_name}
                (col_bigint, col_int, col_varchar, col_decimal, col_date, col_datetime, col_boolean, col_json)
                VALUES
                (9223372036854775807, 2147483647, 'test', 12345.67, '2024-01-01', '2024-01-01 12:00:00', true, '{"key": "value"}')
                """
            )

            count = starrocks_adapter.fetchone(
                f"SELECT COUNT(*) FROM {table_name}"
            )
            assert count[0] == 1, "Data insertion with various types failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "id": exp.DataType.build("BIGINT"),
                    "name": exp.DataType.build("VARCHAR(100)"),
                },
                primary_key=("id",),
            )

            # Verify table exists
            result = starrocks_adapter.fetchone(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_pk_table'"
            )
            assert result is not None, "CREATE TABLE with PRIMARY KEY failed"

            # Test PRIMARY KEY constraint works (insert duplicate should fail)
            starrocks_adapter.execute(
                f"INSERT INTO {table_name} (id, name) VALUES (1, 'Alice')"
            )
            # TODO: Verify PRIMARY KEY constraint behavior
            # StarRocks PRIMARY KEY allows duplicates but keeps latest version

        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)

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
        """Test various data types support."""
        db_name = "sr_test_types_db"
        table_name = f"{db_name}.sr_types_table"

        try:
            starrocks_adapter.create_schema(db_name, ignore_if_exists=True)

            # CREATE TABLE with multiple data types
            starrocks_adapter.create_table(
                table_name,
                target_columns_to_types={
                    "col_bigint": exp.DataType.build("BIGINT"),
                    "col_int": exp.DataType.build("INT"),
                    "col_varchar": exp.DataType.build("VARCHAR(200)"),
                    "col_decimal": exp.DataType.build("DECIMAL(18,2)"),
                    "col_date": exp.DataType.build("DATE"),
                    "col_datetime": exp.DataType.build("DATETIME"),
                    "col_boolean": exp.DataType.build("BOOLEAN"),
                    "col_json": exp.DataType.build("JSON"),
                },
            )

            # Verify all columns created
            columns = starrocks_adapter.fetchall(
                f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = 'sr_types_table' "
                f"ORDER BY ORDINAL_POSITION"
            )
            assert len(columns) == 8, "Not all columns created"

            # Test data insertion with various types
            starrocks_adapter.execute(
                f"""
                INSERT INTO {table_name}
                (col_bigint, col_int, col_varchar, col_decimal, col_date, col_datetime, col_boolean, col_json)
                VALUES
                (9223372036854775807, 2147483647, 'test', 12345.67, '2024-01-01', '2024-01-01 12:00:00', true, '{"key": "value"}')
                """
            )

            count = starrocks_adapter.fetchone(
                f"SELECT COUNT(*) FROM {table_name}"
            )
            assert count[0] == 1, "Data insertion with various types failed"
        finally:
            starrocks_adapter.drop_schema(db_name, ignore_if_not_exists=True)
