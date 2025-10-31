# SQLMesh Test Specifications for StarRocks Engine Adapter

## Overview

This guide outlines the testing conventions and requirements for adding StarRocks adapter tests to the SQLMesh test suite.

## Test File Location

```
tests/core/engine_adapter/test_starrocks.py
```

Following the naming convention:
- `test_<dialect>.py` (e.g., `test_clickhouse.py`, `test_doris.py`)
- Placed in `tests/core/engine_adapter/` directory

## Required Imports and Markers

### Standard Import Pattern

```python
import typing as t

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from pytest_mock.plugin import MockerFixture

# Required pytest markers
pytestmark = [pytest.mark.starrocks, pytest.mark.engine]
```

### Key Components

1. **`pytestmark`**: Required markers for test categorization
   - `pytest.mark.starrocks`: Identifies StarRocks-specific tests
   - `pytest.mark.engine`: Identifies engine adapter tests

2. **`to_sql_calls()`**: Helper function from `tests.core.engine_adapter`
   - Extracts SQL from mocked cursor execute calls
   - Returns list of SQL strings for assertion

3. **`make_mocked_engine_adapter`**: Fixture from `conftest.py`
   - Creates mocked adapter instance for testing
   - No real database connection needed

## Test Structure

### 1. Fixture Pattern

```python
@pytest.fixture
def adapter(make_mocked_engine_adapter) -> StarRocksEngineAdapter:
    """Create a mocked StarRocks adapter for testing."""
    return make_mocked_engine_adapter(StarRocksEngineAdapter)
```

**Alternative**: Use inline fixture call in each test:

```python
def test_create_schema(make_mocked_engine_adapter: t.Callable[..., StarRocksEngineAdapter]):
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
    # test code...
```

### 2. Test Method Pattern

```python
def test_<method_name>(adapter: StarRocksEngineAdapter):
    """Test description."""
    # Arrange: Set up test data
    
    # Act: Call adapter method
    adapter.some_method(args)
    
    # Assert: Verify SQL generated
    assert to_sql_calls(adapter) == [
        "EXPECTED SQL STATEMENT 1",
        "EXPECTED SQL STATEMENT 2",
    ]
```

### 3. Common Test Categories

Based on Doris and ClickHouse examples:

#### A. Schema Operations
```python
def test_create_schema(adapter: StarRocksEngineAdapter):
    adapter.create_schema("test_schema")
    assert to_sql_calls(adapter) == [
        "CREATE DATABASE IF NOT EXISTS `test_schema`",
    ]

def test_drop_schema(adapter: StarRocksEngineAdapter):
    adapter.drop_schema("test_schema")
    adapter.drop_schema("test_schema", ignore_if_not_exists=False)
    assert to_sql_calls(adapter) == [
        "DROP DATABASE IF EXISTS `test_schema`",
        "DROP DATABASE `test_schema`",
    ]
```

#### B. Table Operations
```python
def test_create_table(adapter: StarRocksEngineAdapter):
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test description"},
        table_description="test table",
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test description') COMMENT 'test table'",
    ]

def test_create_table_with_primary_key(adapter: StarRocksEngineAdapter):
    adapter.create_table(
        "test_table",
        target_columns_to_types={"id": exp.DataType.build("INT"), "name": exp.DataType.build("VARCHAR(100)")},
        primary_key=("id",),
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`id` INT, `name` VARCHAR(100)) PRIMARY KEY (`id`)",
    ]

def test_rename_table(adapter: StarRocksEngineAdapter):
    adapter.rename_table("old_table", "new_table")
    # Use execute.assert_called_once_with for non-expression calls
    adapter.cursor.execute.assert_called_once_with(
        "ALTER TABLE `old_table` RENAME `new_table`"
    )
```

#### C. View Operations
```python
def test_create_view(adapter: StarRocksEngineAdapter):
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)
    
    assert to_sql_calls(adapter) == [
        "DROP VIEW IF EXISTS `test_view`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
    ]
```

#### D. Data Manipulation
```python
def test_delete_from(adapter: StarRocksEngineAdapter):
    adapter.delete_from(exp.to_table("foo"), "a = 1")
    assert to_sql_calls(adapter) == [
        "DELETE FROM `foo` WHERE `a` = 1",
    ]
```

### 4. Using Mocker for Dynamic Behavior

```python
def test_with_mocked_methods(
    adapter: StarRocksEngineAdapter, 
    mocker: MockerFixture
):
    # Mock specific method behavior
    mocker.patch.object(adapter, "columns", return_value={
        "a": exp.DataType.build("INT"),
        "b": exp.DataType.build("STRING"),
    })
    
    # Test with mocked behavior
    result = adapter.some_method()
    # assertions...
```

### 5. Testing Table Properties

StarRocks-specific features (PRIMARY KEY, partitioning, etc.):

```python
def test_create_table_with_partition(adapter: StarRocksEngineAdapter):
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "dt": exp.DataType.build("DATE"),
        },
        partitioned_by=[exp.column("dt")],
    )
    # Verify partition clause in generated SQL
    sql = to_sql_calls(adapter)[0]
    assert "PARTITION BY" in sql
```

## Assertion Patterns

### 1. SQL List Assertion

```python
assert to_sql_calls(adapter) == [
    "EXPECTED SQL 1",
    "EXPECTED SQL 2",
]
```

### 2. Single Call Assertion

```python
adapter.cursor.execute.assert_called_once_with("EXPECTED SQL")
```

### 3. Call Count Assertion

```python
assert adapter.cursor.execute.call_count == 3
```

### 4. Partial SQL Matching

```python
sql = to_sql_calls(adapter)[0]
assert "CREATE TABLE" in sql
assert "PRIMARY KEY" in sql
```

### 5. With Identifier Quoting

```python
# Default: identify=True (backticks for MySQL-style)
assert to_sql_calls(adapter) == ["`table_name`"]

# Custom identifier handling
assert to_sql_calls(adapter, identify=False) == ["table_name"]
```

## Test Markers in pyproject.toml

Already added for StarRocks:

```toml
[tool.pytest.ini_options]
markers = [
    # ...
    "starrocks: test for StarRocks",
    # ...
]
```

## Running Tests

### Run All StarRocks Tests

```bash
pytest tests/core/engine_adapter/test_starrocks.py -v
```

### Run Specific Test

```bash
pytest tests/core/engine_adapter/test_starrocks.py::test_create_schema -v
```

### Run with Markers

```bash
# All StarRocks tests
pytest -m starrocks -v

# All engine adapter tests
pytest -m engine -v

# StarRocks AND engine
pytest -m "starrocks and engine" -v
```

### Run Fast Tests Only

```bash
# Unit tests (mocked, no real DB)
pytest -m fast tests/core/engine_adapter/test_starrocks.py
```

## Integration Tests (Optional)

For tests requiring real StarRocks database:

### Location
```
tests/core/engine_adapter/integration/test_starrocks_integration.py
```

### Markers
```python
pytestmark = [
    pytest.mark.starrocks, 
    pytest.mark.engine, 
    pytest.mark.docker  # or pytest.mark.remote
]
```

### Docker Compose Setup
```
tests/core/engine_adapter/integration/docker/compose.starrocks.yaml
```

Example structure (like ClickHouse):
```yaml
version: '3.8'
services:
  starrocks-fe:
    image: starrocks/fe-ubuntu:latest
    ports:
      - "9030:9030"
    environment:
      - FE_SERVERS=fe1:172.26.92.140:9010
      
  starrocks-be:
    image: starrocks/be-ubuntu:latest
    depends_on:
      - starrocks-fe
```

## Best Practices

### 1. Test Naming

- **DO**: `test_create_schema`, `test_drop_table`, `test_primary_key_support`
- **DON'T**: `test_1`, `test_functionality`, `my_test`

### 2. One Concept Per Test

```python
# GOOD: Test one feature
def test_create_table_with_comment():
    adapter.create_table("t", {...}, table_description="comment")
    assert "COMMENT 'comment'" in to_sql_calls(adapter)[0]

# AVOID: Testing multiple unrelated features
def test_everything():
    adapter.create_table(...)
    adapter.drop_table(...)
    adapter.create_view(...)
    # Too much in one test!
```

### 3. Clear Expected SQL

```python
# GOOD: Clear, readable SQL
assert to_sql_calls(adapter) == [
    "CREATE DATABASE IF NOT EXISTS `test_db`",
]

# AVOID: Complex string manipulation
expected = "CREATE " + "DATABASE" + " IF NOT EXISTS"
assert expected in to_sql_calls(adapter)[0]
```

### 4. Reset Mock Between Tests

```python
def test_first_operation(adapter: StarRocksEngineAdapter):
    adapter.create_schema("db1")
    assert to_sql_calls(adapter) == ["CREATE DATABASE IF NOT EXISTS `db1`"]

def test_second_operation(adapter: StarRocksEngineAdapter):
    # Adapter fixture is fresh for each test
    adapter.create_schema("db2")
    assert to_sql_calls(adapter) == ["CREATE DATABASE IF NOT EXISTS `db2`"]
```

### 5. Test Edge Cases

```python
def test_create_schema_without_if_exists():
    adapter = make_mocked_engine_adapter(StarRocksEngineAdapter)
    adapter.create_schema("test", ignore_if_exists=False)
    assert to_sql_calls(adapter) == [
        "CREATE DATABASE `test`",  # No IF NOT EXISTS
    ]
```

## Complete Example Template

```python
"""
Tests for StarRocks Engine Adapter

Tests cover:
- Schema operations (CREATE/DROP DATABASE)
- Table operations (CREATE/DROP/ALTER/RENAME TABLE)
- View operations (CREATE/DROP VIEW)
- Primary Key support
- Partition support
- Data manipulation (INSERT/DELETE/UPDATE)
"""

import typing as t

import pytest
from sqlglot import expressions as exp
from sqlglot import parse_one

from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter
from pytest_mock.plugin import MockerFixture

pytestmark = [pytest.mark.starrocks, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter) -> StarRocksEngineAdapter:
    """Create a mocked StarRocks adapter for testing."""
    return make_mocked_engine_adapter(StarRocksEngineAdapter)


def test_create_schema(adapter: StarRocksEngineAdapter):
    """Test CREATE DATABASE statement generation."""
    adapter.create_schema("test_schema")
    
    assert to_sql_calls(adapter) == [
        "CREATE DATABASE IF NOT EXISTS `test_schema`",
    ]


def test_create_table_with_primary_key(adapter: StarRocksEngineAdapter):
    """Test CREATE TABLE with PRIMARY KEY support (StarRocks native feature)."""
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("VARCHAR(100)"),
        },
        primary_key=("id",),
    )
    
    sql = to_sql_calls(adapter)[0]
    assert "CREATE TABLE IF NOT EXISTS `test_table`" in sql
    assert "PRIMARY KEY (`id`)" in sql


def test_create_view(adapter: StarRocksEngineAdapter):
    """Test CREATE VIEW statement generation."""
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    
    assert to_sql_calls(adapter) == [
        "DROP VIEW IF EXISTS `test_view`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
    ]


# Add more tests following this pattern...
```

## Comparison: Doris vs StarRocks

Key differences to test:

| Feature | Doris | StarRocks | Test Focus |
|---------|-------|-----------|------------|
| Primary Key | Converts to UNIQUE KEY | Native PRIMARY KEY | Verify PRIMARY KEY in SQL |
| Partition Syntax | PARTITION BY RANGE | PARTITION BY / PARTITION BY RANGE | Test both syntaxes |
| DELETE | Limited subquery | Primary Key supports subquery | Test DELETE with subquery |
| Transaction | No | No (limited in 3.5+) | Skip transaction tests |

## Checklist for StarRocks Tests

- [ ] `test_create_schema` - Uses DATABASE keyword
- [ ] `test_drop_schema` - With/without IF EXISTS
- [ ] `test_create_table` - Basic table creation
- [ ] `test_create_table_with_primary_key` - PRIMARY KEY support
- [ ] `test_create_table_with_comment` - Table/column comments
- [ ] `test_create_table_with_partition` - PARTITION BY
- [ ] `test_create_view` - Basic view
- [ ] `test_create_materialized_view` - If supported
- [ ] `test_rename_table` - ALTER TABLE RENAME
- [ ] `test_delete_from` - DELETE statement
- [ ] `test_create_index` - If supported
- [ ] `test_alter_table` - ADD/DROP COLUMN

## Next Steps

1. Create `tests/core/engine_adapter/test_starrocks.py`
2. Start with basic schema/table tests
3. Add StarRocks-specific features (PRIMARY KEY, partitions)
4. Run tests: `pytest tests/core/engine_adapter/test_starrocks.py -v`
5. Add integration tests if needed (with real StarRocks instance)

## Reference Files

- **Doris Tests**: `tests/core/engine_adapter/test_doris.py` (closest reference)
- **ClickHouse Tests**: `tests/core/engine_adapter/test_clickhouse.py` (cluster mode examples)
- **Base Tests**: `tests/core/engine_adapter/test_base.py` (common patterns)
- **Conftest**: `tests/conftest.py` (fixtures and helpers)
- **Helper**: `tests/core/engine_adapter/__init__.py` (`to_sql_calls` function)

Happy testing! 🚀
