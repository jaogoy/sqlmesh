# Integration Test: Quote Character Handling

## Overview

Added a comprehensive integration test `test_e2e_quote_character_handling` to verify that MODEL parsing correctly handles different quote types in StarRocks physical properties.

## Test Location

File: [`tests/core/engine_adapter/integration/test_integration_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/integration/test_integration_starrocks.py)

Class: `TestEndToEndModelParsing`

Method: `test_e2e_quote_character_handling`

## What This Test Verifies

### 1. Quote Parsing Behavior

The test uses a MODEL definition with **different quote types** to verify parsing:

```sql
MODEL (
    name test.quote_handling_model,
    physical_properties (
        -- Single quotes (correct) → Literal
        primary_key = 'id, dt',

        -- Double quotes (parser quirk) → Column(quoted=True)
        -- But our ensure_parenthesized handles this
        order_by = "id, region",

        -- String with structured syntax
        distributed_by = 'HASH(customer_id) BUCKETS 8',

        -- Generic properties
        replication_num = '1',
        storage_medium = 'HDD'
    )
);
```

### 2. End-to-End Pipeline

The test verifies the **complete pipeline**:

```
MODEL Definition (with quotes)
    ↓
d.parse() + load_sql_based_model()
    ↓
model.physical_properties
    ├─ primary_key: exp.Literal or exp.Column (depending on quotes)
    ├─ order_by: exp.Literal or exp.Column (depending on quotes)
    └─ distributed_by: exp.Literal
    ↓
adapter.create_table(table_properties=model.physical_properties)
    ↓
PropertyValidator.validate_and_normalize_property(
    preprocess_parentheses=True  ← Calls ensure_parenthesized
)
    ↓
ensure_parenthesized()
    ├─ Handle Literal.string → extract string
    ├─ Handle Column(quoted=True) → extract name (WORKAROUND)
    └─ Wrap in parentheses if needed
    ↓
SQL Generation
    ↓
Execute on Real StarRocks
    ↓
Verify via SHOW CREATE TABLE
```

### 3. Assertions

The test makes **precise assertions** on the generated DDL:

#### 1. PRIMARY KEY (from single quotes)
```python
# From: primary_key = 'id, dt'
pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", ddl)
assert "id" in pk_cols and "dt" in pk_cols
```

**Expected**: `PRIMARY KEY (id, dt)`

#### 2. ORDER BY (from double quotes - tests workaround)
```python
# From: order_by = "id, region"
order_match = re.search(r"ORDER BY\s*\(([^)]+)\)", ddl)
assert "id" in order_cols and "region" in order_cols
```

**Expected**: `ORDER BY (id, region)`

**Critical**: This verifies our `Column(quoted=True)` workaround works!

#### 3. DISTRIBUTED BY (from single quotes)
```python
# From: distributed_by = 'HASH(customer_id) BUCKETS 8'
assert "DISTRIBUTED BY HASH" in ddl
assert "customer_id" in ddl
assert "BUCKETS 8" in ddl
```

**Expected**: `DISTRIBUTED BY HASH (customer_id) BUCKETS 8`

#### 4. PROPERTIES (generic properties)
```python
assert "replication_num" in ddl
assert "storage_medium" in ddl or "HDD" in ddl
```

**Expected**: `PROPERTIES ('replication_num'='1', 'storage_medium'='HDD')`

### 4. Functional Test

The test also performs a **functional verification**:

```python
# Insert data
starrocks_adapter.execute(
    f"INSERT INTO {table_name} "
    f"(id, dt, region, customer_id) "
    f"VALUES (100, '2024-01-01', 'US', 1001)"
)

# Query data
result = starrocks_adapter.fetchone(
    f"SELECT id, region, customer_id FROM {table_name} WHERE id = 100"
)
assert result == (100, "US", 1001)
```

This ensures the table is **actually functional**, not just syntactically correct.

## Quote Behavior Reference

### MySQL/StarRocks SQL

| Quote Type | Purpose | Example |
|------------|---------|---------|
| `` ` `` | Identifier | `` `column_name` `` |
| `'` | String Literal | `'hello world'` |
| `"` | String (default) or Identifier (ANSI_QUOTES) | `"value"` |

### SQLMesh MODEL Parsing

| Quote Type | Parsed As | Type |
|------------|-----------|------|
| `'value'` | Literal | `exp.Literal(is_string=True)` ✓ |
| `"value"` | Column | `exp.Column(quoted=True)` ✗ (inconsistent) |
| `value` | Column | `exp.Column(quoted=False)` ✓ |

## Why This Test Is Important

### 1. Real-World Usage
Users might write MODEL definitions with either single or double quotes. This test ensures **both work correctly**.

### 2. Parser Quirk Coverage
The test specifically covers the **parser quirk** where double quotes in MODEL context are treated as identifiers instead of strings.

### 3. Workaround Verification
Our `ensure_parenthesized` function has special handling for `Column(quoted=True)`. This test verifies that workaround **actually works** in production.

### 4. Regression Prevention
If someone changes the quote handling logic, this test will **catch regressions** immediately.

## Debugging Features

The test includes extensive logging:

```python
logger.info(f"Parsed physical_properties: {params['table_properties']}")
for key, value in params['table_properties'].items():
    logger.info(f"  {key}: {type(value).__name__} = {value}")

logger.info(f"Quote Handling Test DDL:\n{ddl}")

logger.info("✓ Quote handling test PASSED:")
logger.info("  - Single quotes 'value' → parsed correctly")
logger.info("  - Double quotes \"value\" → handled by workaround")
logger.info("  - All clauses generated correctly")
logger.info("  - Table functional test passed")
```

This helps diagnose issues when the test fails.

## Running the Test

### With Docker (recommended)
```bash
pytest -m "starrocks and docker" \
  tests/core/engine_adapter/integration/test_integration_starrocks.py::TestEndToEndModelParsing::test_e2e_quote_character_handling \
  -xvs
```

### With Local StarRocks
```bash
export STARROCKS_HOST=localhost
export STARROCKS_PORT=9030
export STARROCKS_USER=root
export STARROCKS_PASSWORD=""

pytest \
  tests/core/engine_adapter/integration/test_integration_starrocks.py::TestEndToEndModelParsing::test_e2e_quote_character_handling \
  -xvs
```

## Expected Output

When the test passes, you should see:

```
INFO     Parsed physical_properties: {
  'primary_key': Literal(...),
  'order_by': Column(...),
  'distributed_by': Literal(...),
  ...
}
INFO     Quote Handling Test DDL:
CREATE TABLE `sr_e2e_quote_handling_db`.`sr_quote_test_table` (
  `id` bigint(20) NOT NULL,
  `dt` date NOT NULL,
  `region` varchar(50) NOT NULL,
  `customer_id` int(11) NOT NULL
) ENGINE=OLAP
PRIMARY KEY (`id`, `dt`)
DISTRIBUTED BY HASH (`customer_id`) BUCKETS 8
ORDER BY (`id`, `region`)
PROPERTIES (
  "replication_num" = "1",
  "storage_medium" = "HDD"
)

INFO     ✓ Quote handling test PASSED:
INFO       - Single quotes 'value' → parsed correctly
INFO       - Double quotes "value" → handled by workaround
INFO       - All clauses generated correctly
INFO       - Table functional test passed
```

## Related Files

- Implementation:
  - [`/sqlmesh/core/engine_adapter/starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py) - `ensure_parenthesized()` function

- Tests:
  - [`/tests/core/engine_adapter/test_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py) - Unit tests
  - [`/tests/core/engine_adapter/test_quote_parsing.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_quote_parsing.py) - Quote parsing tests

- Documentation:
  - [`/docs/quote_character_handling.md`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/docs/quote_character_handling.md) - Quote behavior explanation
  - [`/docs/test_improvement_order_by.md`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/docs/test_improvement_order_by.md) - Order by test improvement

## Summary

This integration test provides **comprehensive coverage** of quote character handling in StarRocks MODEL parsing, from initial parsing through SQL generation to actual database execution. It ensures that both single and double quotes work correctly, and verifies our workaround for the parser quirk.
