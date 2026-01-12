# StarRocks Properties Test Design (Table / View / Materialized View)

## Overview

This document defines the testing strategy for StarRocks properties used in DDL generation, covering:

- Table physical properties (e.g., keys, partition, distribution)
- View properties (e.g., `SECURITY`)
- Materialized view (MV) properties (e.g., `REFRESH` clause)

It covers both **integration tests** (end-to-end with real DB) and **unit tests** (function-level with parsed expressions).

## Testing Strategy: Two-Tier Approach

### Tier 1: Integration Tests (Slow, Representative Complex Scenarios)

- Full pipeline: `MODEL definition → Parse → create_table() → SHOW CREATE TABLE → Verify DDL`
- Validates that parsed expression types work correctly with real StarRocks
- **Focus on representative COMPLEX scenarios** (e.g., multi-column, structured forms, function expressions)
- Fewer tests, but each covers critical paths through combination coverage

### Tier 2: Unit Tests (Fast, Complete Coverage)

- Use `parse_one()` or `d.parse()` to get real expressions (same as system produces)
- Test individual functions: `_build_table_properties_exp()`, `_build_partition_property()`, etc.
- **Must cover ALL possibilities** including:
  - All value forms (tuple, string, structured)
  - String form handling (with/without parentheses, multi-column)
  - Edge cases and boundaries

---

## Property Value Coverage Matrix

> Properties are organized in order: **Table Key → Partition → Distribution → Order By → Comment → Properties**

### 1. Key Types (`primary_key`, `duplicate_key`, `unique_key`, `aggregate_key`)

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Single identifier | `primary_key = id` | physical_properties | ✅ | ✅ |
| Tuple (multi-column) | `primary_key = (id, dt)` | physical_properties | | ✅ |
| String paren | `primary_key = "(id, dt)"` | physical_properties | ✅ | ✅ |
| String no-paren (auto-wrap) | `primary_key = "id, dt"` | physical_properties | ✅ | ✅ |
| String with paren | `primary_key = "(id, dt)"` | physical_properties | | ✅ |
| duplicate_key tuple | `duplicate_key = (id, dt)` | physical_properties | ✅ | ✅ |
| unique_key tuple | `unique_key = (id, dt)` | physical_properties | ✅ | ✅ |
| aggregate_key tuple | `aggregate_key = (id, dt)` | physical_properties | ✅ | ✅ |

**Key Points**:

- String without parentheses (`"id, dt"`) is auto-wrapped to `"(id, dt)"` during parsing
- All four key types share the same value form handling
- Key types are mutually exclusive - only one can be specified per table
- `unique_key` and `aggregate_key` must be tested to ensure proper support

---

### 2. Partition (`partitioned_by` / `partition_by`, `partitions`)

#### 2.1 `partitioned_by` / `partition_by` (Alias)

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Single column | `dt` | model parameter | ✅ | ✅ |
| Single column (tuple) | `(dt)` | model parameter | | ✅ |
| Multi-column | `(year, month)` | model parameter | ✅ | ✅ |
| Multi-column (string) | `"year, month"` | model parameter | ✅ | ✅ |
| Function expression | `(date_trunc('day', ts))` | model parameter | | ✅ |
| Multi-expr with function | `(from_unixtime(ts), region)` | model parameter | ✅ | ✅ |
| RANGE single col | `RANGE(dt)` | model parameter | ✅ | ✅ |
| RANGE multi col (string) | `"RANGE(year, month)"` | model parameter | | ✅ |
| LIST | `LIST(region)` | physical_properties | ✅ | ✅ |
| String multi-expr | `'date_trunc("day", ts), col2'` | physical_properties | | ✅ |
| String multi-expr (paren) | `'(date_trunc("day", ts), col2)'` | physical_properties | | ✅ |
| Alias: partition_by | `partition_by (year, month)` | physical_properties | ✅ | ✅ |

#### 2.2 `partitions` (Partition Values)

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Single RANGE partition (string) | `'PARTITION p1 VALUES LESS THAN ("2024-01-01")'` | physical_properties | ✅ | ✅ |
| Single LIST partition (string) | `'PARTITION p_cn VALUES IN ("cn", "tw")'` | physical_properties | ✅ | ✅ |
| Multiple partitions (tuple of strings) | `('PARTITION p1 ...', 'PARTITION p2 ...')` | physical_properties | ✅ | ✅ |
| Batch partition expression | `'START ("2024-01-01") END ("2024-12-31") EVERY (...)'` | physical_properties | ✅ | ✅ |

**Key Points**:

- `partition_by` is an alias for `partitioned_by`
- Partition expressions can use different functions (RANGE, LIST, from_unixtime, date_trunc, etc.)
- **`partitions` can be a single string OR a tuple of strings, but NOT one string containing multiple PARTITION definitions**
- Partition values (e.g., `PARTITION p1 VALUES LESS THAN (...)`) are specified via `partitions` property, separate from `partitioned_by`

---

### 3. Distribution (`distributed_by`)

| Value Form | Example | Integration | Unit |
|------------|---------|-------------|------|
| String HASH single col | `'HASH(id) BUCKETS 10'` | | ✅ |
| String HASH multi col | `'HASH(id, region) BUCKETS 10'` | ✅ | ✅ |
| String RANDOM | `'RANDOM'` | | ✅ |
| String RANDOM + BUCKETS | `'RANDOM BUCKETS 10'` | | ✅ |
| Structured HASH (quoted kind) | `(kind='HASH', expressions=id, buckets=10)` | ✅ | ✅ |
| Structured HASH (unquoted kind) | `(kind=HASH, expressions=id, buckets=10)` | ✅ | ✅ |
| Structured RANDOM | `(kind=RANDOM, buckets=10)` | ✅ | ✅ |
| Structured multi-column | `(kind=HASH, expressions=(a,b), buckets=10)` | | ✅ |

**Key Points**:

- `kind=HASH` (unquoted identifier) and `kind=RANDOM` must both work
- RANDOM distribution does not require expressions

---

### 4. Order By (`order_by` / `clustered_by`)

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Single column | `id` | physical_properties | | ✅ |
| Tuple (multi-column) | `(id, dt)` | physical_properties | ✅ | ✅ |
| String form single | `"id"` | physical_properties | | ✅ |
| String form multi | `"id, dt"` | physical_properties | | ✅ |
| String form multi (paren) | `"(id, dt)"` | physical_properties | ✅ | ✅ |
| Alias: clustered_by | `clustered_by (id, dt)` | model parameter | ✅ | ✅ |

**Limitation**: StarRocks `ORDER BY` only supports column lists. **NO ASC/DESC allowed**.

---

### 5. Comment

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Table comment | `description = 'Table description'` | model parameter | ✅ | ✅ |
| Column comment | `column_descriptions` | model parameter | ✅ | ✅ |

---

### 6. Generic Table Properties

| Value Form | Example | Integration | Unit |
|------------|---------|-------------|------|
| Integer | `replication_num = 1` | ✅ | ✅ |
| Boolean TRUE | `enable_persistent_index = TRUE` | ✅ | ✅ |
| Boolean FALSE | `in_memory = FALSE` | | ✅ |
| String value | `storage_cooldown_time = '2024-01-01'` | | ✅ |

---

### 7. View Properties (`security`)

StarRocks view syntax supports `SECURITY <value>` (no `=`).

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Identifier (unquoted) | `INVOKER` | view_properties | ✅ | ✅ |
| String literal | `'INVOKER'` | view_properties | | ✅ |
| Case-insensitive input | `invoker` | view_properties | | ✅ |
| NONE | `NONE` | view_properties | ✅ | ✅ |
| DEFINER (version-dependent) | `DEFINER` | view_properties | | ✅ |
| Invalid enum | `'FOO'` | view_properties | | ✅ (error) |

**Key Points**:

- Adapter responsibility: accept supported forms, normalize to the expected enum, and render **`SECURITY <value>`** in `CREATE VIEW`.
- Integration tests should validate **StarRocks accepts the generated DDL** and `SHOW CREATE VIEW` contains the expected clause.
- If `DEFINER` is not supported by the target StarRocks version, keep it as **unit-only** or gate the integration assertion by version/capability.

---

### 8. MV Refresh Properties (`refresh_moment`, `refresh_scheme`)

StarRocks MV syntax supports a `REFRESH ...` clause. In the adapter:

- `refresh_moment` maps to the optional moment token: `IMMEDIATE | DEFERRED`
- `refresh_scheme` is a string that begins with `MANUAL | ASYNC` and may include:
  - `START ('...')`
  - `EVERY (INTERVAL <n> <unit>)`

Because the `refresh_scheme` text contains multiple optional parts, tests should cover parsing/normalization and final DDL rendering for representative forms.

#### 8.1 `refresh_moment`

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| Identifier | `IMMEDIATE` | view_properties (MV) | ✅ | ✅ |
| String literal | `'DEFERRED'` | view_properties (MV) | | ✅ |
| Case-insensitive input | `deferred` | view_properties (MV) | | ✅ |
| Invalid enum | `'AUTO'` | view_properties (MV) | | ✅ (error) |

#### 8.2 `refresh_scheme`

| Value Form | Example | Location | Integration | Unit |
|------------|---------|----------|-------------|------|
| ASYNC only | `'ASYNC'` | view_properties (MV) | ✅ | ✅ |
| ASYNC + START (quote form) | `"ASYNC START ('2025-01-01 00:00:00')"` | view_properties (MV) | ✅ | ✅ |
| ASYNC + EVERY | `'ASYNC EVERY (INTERVAL 5 MINUTE)'` | view_properties (MV) | ✅ | ✅ |
| ASYNC + START + EVERY | `'ASYNC START ("2025-01-01 00:00:00") EVERY (INTERVAL 5 MINUTE)'` | view_properties (MV) | ✅ | ✅ |
| Case-insensitive keywords | `'async start (2025-01-01) every (interval 5 minute)'` | view_properties (MV) | | ✅ |
| Invalid prefix | `'SCHEDULE ...'` | view_properties (MV) | | ✅ (error) |
| Malformed EVERY | `'ASYNC EVERY (INTERVAL X MINUTE)'` | view_properties (MV) | | ✅ (error) |
| MANUAL only | `'MANUAL'` | view_properties (MV) | ✅ | ✅ |

**Key Points**:

- Adapter responsibility: validate/normalize and generate **syntactically correct** `REFRESH ...` DDL.
- Integration tests should verify `CREATE MATERIALIZED VIEW` succeeds and `SHOW CREATE MATERIALIZED VIEW` contains the expected `REFRESH` clause.
- The adapter intentionally does **not** validate whether StarRocks will *semantically* refresh data as expected; that is engine behavior.

---

### 9. Invalid / Error Case Coverage (Guideline)

In addition to happy-path value-form coverage, add **targeted invalid tests** for properties where the adapter performs validation or parsing. Prefer unit tests for error cases, and keep integration tests focused on representative valid combinations.

Recommended invalid categories (pick the most relevant per property):

- **Invalid enum/token**: e.g., unknown `security` or `refresh_moment`
- **Malformed structured string**: e.g., `refresh_scheme` missing required prefix or has malformed `EVERY (INTERVAL ...)`
- **Wrong type form**: e.g., list/tuple where only a scalar is supported
- **Mutual exclusivity / alias conflicts**: where the property system defines aliases or exclusive groups

#### 9.1 Table Properties: Recommended Invalid Cases (Unit)

Only add invalid tests where SQLMesh / adapter layer has explicit rules, parsing, or conflict checks (avoid testing StarRocks engine semantics).

- **Key types (mutual exclusivity)**:
  - Defining 2+ of `primary_key` / `duplicate_key` / `unique_key` / `aggregate_key` at the same time should fail.
- **Aliases (mutual exclusivity)**:
  - Defining both `partitioned_by` and `partition_by` should fail.
  - Defining both `clustered_by` and `order_by` should fail.
- **Common mis-typed property names (invalid name detection)**:
  - `partition` (should be `partitioned_by` / `partition_by`)
  - `distribution` / `distribute` (should be `distributed_by`)
  - `order` / `ordering` (should be `order_by` / `clustered_by`)
- **Partition definitions (`partitions`)**:
  - Reject a single string containing multiple `PARTITION ...` definitions (must be tuple-of-strings).
  - If partition kind is expression-based (not RANGE/LIST), reject providing `partitions` (if the adapter enforces this).
- **Distribution parsing (`distributed_by`)**:
  - `BUCKETS` with non-numeric value (e.g., `BUCKETS X`) should fail.
  - Malformed strings that cannot be parsed as `HASH(...)` / `RANDOM` should fail.
- **Order by limitations (`order_by` / `clustered_by`)**:
  - If `ASC/DESC` is provided and the adapter disallows it, the adapter should fail fast with a clear error.

#### 9.2 View / MV: Recommended Invalid Cases (Unit)

- **View security (`security`)**:
  - Unknown enum (e.g., `FOO`) should fail.
- **MV refresh (`refresh_moment`, `refresh_scheme`)**:
  - Unknown enum for `refresh_moment` should fail.
  - `refresh_scheme` must start with `MANUAL` or `ASYNC` (otherwise fail).
  - Malformed `EVERY (INTERVAL ...)` (non-numeric interval, missing unit) should fail.

---

## Integration Test Cases (Representative Complex Scenarios)

Based on the matrix above, select **representative complex scenarios** for integration tests:

### Test Case 1: Model Parameters (Multi-column)

**Covers**: `partitioned_by` (multi-expr with function), `clustered_by` (multi-column)

```sql
MODEL (
    partitioned_by (from_unixtime(ts), region),  -- Multi-expr with function
    clustered_by (order_id, event_date)          -- Multi-column
)
```

### Test Case 2: Physical Properties Core

**Covers**: `primary_key` (tuple), `distributed_by` (string multi-col), `order_by` (tuple), generic props

```sql
physical_properties (
    primary_key = (order_id, event_date),
    distributed_by = 'HASH(customer_id, region) BUCKETS 16',  -- Multi-column string
    order_by = (order_id, region),
    replication_num = 1,
    enable_persistent_index = TRUE
)
```

### Test Case 3: String No-Paren Auto-Wrap

**Covers**: `primary_key = "id, dt"` auto-conversion (multi-column string)

```sql
physical_properties (
    primary_key = "order_id, event_date",  -- No parentheses, auto-wrapped
    distributed_by = 'HASH(order_id) BUCKETS 10'
)
```

### Test Case 4: Structured Distribution

**Covers**: `kind=HASH` (unquoted), `kind=RANDOM`

```sql
-- Test A: Unquoted HASH with multi-column
distributed_by = (kind=HASH, expressions=(customer_id, region), buckets=16)

-- Test B: RANDOM
distributed_by = (kind=RANDOM, buckets=10)
```

### Test Case 5: Partition with RANGE

**Covers**: `partitioned_by` RANGE, `partitions` with values

```sql
MODEL (
    partitioned_by RANGE(year, month),  -- Multi-column RANGE
    physical_properties (
        primary_key = (id, year, month),
        partitions = (
            'PARTITION p202401 VALUES LESS THAN ("2024", "02")',
            'PARTITION p202402 VALUES LESS THAN ("2024", "03")'
        ),
        distributed_by = 'HASH(id) BUCKETS 10'
    )
)
```

### Test Case 6: Partition with LIST

**Covers**: `partitioned_by` LIST, `partitions` with values

```sql
MODEL (
    partitioned_by LIST(region),
    physical_properties (
        primary_key = (id, region),
        partitions = (
            'PARTITION p_cn VALUES IN ("cn", "tw", "hk")',
            'PARTITION p_us VALUES IN ("us", "ca")'
        ),
        distributed_by = 'HASH(id) BUCKETS 10'
    )
)
```

### Test Case 7: Other Key Types

**Covers**: `duplicate_key`, `unique_key`, `aggregate_key`

```sql
-- Test A: duplicate_key
physical_properties (
    duplicate_key = (id, dt),
    distributed_by = 'HASH(id) BUCKETS 10'
)

-- Test B: unique_key
physical_properties (
    unique_key = (id, dt),
    distributed_by = 'HASH(id) BUCKETS 10'
)

-- Test C: aggregate_key
physical_properties (
    aggregate_key = (id, dt),
    distributed_by = 'HASH(id) BUCKETS 10'
)
```

### Test Case 8: CREATE VIEW with SECURITY

**Covers**: `security` value normalization + correct `SECURITY <value>` clause rendering

- Create a base table
- Create a view with `security = INVOKER` (or `NONE`)
- Verify `SHOW CREATE VIEW` contains `SECURITY INVOKER` and the view is queryable

### Test Case 9: CREATE MATERIALIZED VIEW with REFRESH (moment + scheme)

**Covers**: `refresh_moment` + `refresh_scheme` parsing and rendering

- Create a base table
- Create an MV with `refresh_moment = IMMEDIATE` and `refresh_scheme = ASYNC START (...) EVERY (INTERVAL ... ...)`
- Verify `SHOW CREATE MATERIALIZED VIEW` contains `REFRESH IMMEDIATE ASYNC ... START ... EVERY ...`

### Test Case 10: CREATE MATERIALIZED VIEW with REFRESH (scheme only)

**Covers**: scheme-only rendering (no moment token)

- Create an MV with `refresh_scheme = MANUAL` (no `refresh_moment`)
- Verify `SHOW CREATE MATERIALIZED VIEW` contains `REFRESH MANUAL` (or the StarRocks-equivalent rendering)

### Test Case 11: CREATE MATERIALIZED VIEW (Combo A: Model Parameters + Properties Blocks)

**Covers**: MV end-to-end DDL with a representative combination of:

- Create a base table
- Configure MV partition/clustering via **model parameters** (where SQLMesh model syntax supports this for MV kind)
- Configure MV distribution / properties / refresh via **properties blocks** (e.g., physical/view properties)
- Include table + column comments
- Verify `SHOW CREATE MATERIALIZED VIEW` contains expected partition / order / distributed / properties / comment / refresh clauses and the MV is queryable

### Test Case 12: CREATE MATERIALIZED VIEW (Combo B: All-in-Properties Form)

**Covers**: MV end-to-end DDL where most configuration is supplied through properties blocks (minimal model parameters).

- Create a base table
- Create an MV with partition/clustering configured via MV properties block (rather than model parameters)
- Configure distribution / other properties / refresh via MV properties
- Include table + column comments
- Verify `SHOW CREATE MATERIALIZED VIEW` contains expected clauses and the MV is queryable

---

## Test Implementation Approach

### Integration Tests (Isolated Functions)

- One test function per scenario
- Clear failure isolation and simple assertion logic
- Located in `test_integration_starrocks.py`

### Unit Tests (Parametrized)

- Use `@pytest.mark.parametrize` for similar cases within each property
- Different properties have separate test classes/functions
- Located in `test_starrocks.py`

---

## Test File Organization

> Test classes are organized in order: **Table Key → Partition → Distribution → Order By → Comment → Properties**

```text
tests/core/engine_adapter/
├── test_starrocks.py                    # Unit tests (parametrized, complete coverage)
│   ├── TestSchemaOperations              # Existing: Schema/DB operations
│   ├── TestTableOperations               # Existing: Basic table operations
│   ├── TestKeyPropertyBuilding           # Table key types (primary_key, duplicate_key, etc.)
│   ├── TestPartitionPropertyBuilding     # Partition (partitioned_by, partitions)
│   ├── TestDistributionPropertyBuilding  # Distribution (distributed_by)
│   ├── TestOrderByPropertyBuilding       # Order By (order_by, clustered_by)
│   ├── TestViewPropertyBuilding          # View properties (security)
│   ├── TestMVRefreshPropertyBuilding     # MV refresh (refresh_moment, refresh_scheme)
│   └── TestGenericPropertyBuilding       # Generic properties (replication_num, etc.)
│
└── integration/
    └── test_integration_starrocks.py    # Integration tests (isolated, representative)
        ├── TestBasicOperations              # Existing: Basic CRUD operations
        ├── TestTableFeatures                # Existing: Table features
        └── TestEndToEndModelParsing         # E2E model parsing tests
            ├── test_e2e_key_types               # Table key types
            ├── test_e2e_partition_range         # Partition RANGE
            ├── test_e2e_partition_list          # Partition LIST
            ├── test_e2e_distribution_structured # Distribution structured forms
            ├── test_e2e_order_by                # Order By / Clustered By
            ├── test_e2e_view_security            # View SECURITY
            ├── test_e2e_mv_refresh               # MV REFRESH
            └── test_e2e_comprehensive           # All properties combined
```

---

## Verification Points

For each integration test, verify:

1. **DDL contains expected clauses** (PRIMARY KEY, DISTRIBUTED BY, etc.)
2. **Actual column names** appear in the correct clause (not just keyword presence)
3. **Value correctness** (e.g., BUCKETS 16, multi-column list)
4. **Table is functional** (INSERT/SELECT works)

---

## Summary: Test Coverage

| Property | Integration Tests | Unit Tests |
|----------|-------------------|------------|
| primary_key | 2 cases | 4 cases |
| duplicate_key | 1 case | 2 cases |
| unique_key | 1 case | 2 cases |
| aggregate_key | 1 case | 2 cases |
| partitioned_by | 3 cases | 7 cases |
| partitions | 2 cases | 4 cases |
| distributed_by | 4 cases | 8 cases |
| order_by | 2 cases | 4 cases |
| clustered_by | 1 case | 2 cases |
| comment | 1 case | 2 cases |
| generic props | 1 case | 4 cases |
| security (view) | 1 case | 6-7 cases |
| refresh (MV) | 2 cases | 8-10 cases |
| **Total** | **~23-24 integration tests** | **~55-60 unit tests** |
