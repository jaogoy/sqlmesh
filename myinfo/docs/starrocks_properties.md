# StarRocks Model Properties Guide

## Overview

SQLMesh provides two ways to configure StarRocks table properties:

1. **Model parameters** (DDL-level): Direct properties in the `MODEL()` definition
   - Example: `partitioned_by`, `clustered_by`, `primary_key`
   - Go through SQLMesh validation
   - Limited to generic, cross-engine syntax

2. **Physical properties** (Engine-specific): Properties inside `physical_properties (...)` block
   - Example: `partition_by`, `distributed_by`, `order_by`, `duplicate_key`
   - Bypass model-level validation
   - Support advanced engine-specific syntax
   - **More powerful** for StarRocks-specific features

### When to use physical_properties

Use `physical_properties` when:

- Model parameters are **too restrictive** (e.g., `RANGE(col1, col2)` with multiple columns, it can't pass the validator)
- You need **StarRocks-specific features** (e.g., `DISTRIBUTED BY HASH`)
- You want to use **native StarRocks syntax** without SQLMesh transformation

### Key Differences

| Aspect | Model Parameters | Physical Properties |
|--------|------------------|---------------------|
| **Location** | `MODEL(partitioned_by = ...)` | `physical_properties (partition_by = ...)` |
| **Validation** | Strict (e.g., single column only) | Flexible (engine adapter handles) |
| **Portability** | Cross-engine compatible | Engine-specific |
| **Power** | Basic syntax | Advanced features |
| **Example** | `partitioned_by col1` | `partition_by = RANGE(col1, col2)` |

## Model Properties Syntax Design Principles

These principles guide how StarRocks properties are designed in SQLMesh:

### 1. Structured Data Over Strings (Preferred)

**Principle**: Use structured expressions for complex parameters whenever possible.

**Why**: Structured data is:

- Type-safe and validatable
- Easier to parse and transform
- Self-documenting

**Example**:

```sql
-- ✅ Preferred: Structured
physical_properties (
  distributed_by = (kind='HASH', expressions=id, buckets=10)
)

-- ✅ Also supported: String fallback
physical_properties (
  distributed_by = "HASH(id) BUCKETS 10"
)
```

### 2. String Fallback Support (Compatibility)

**Principle**: Always support string values as a fallback option.

**Why**: Provides flexibility when:

- Complex syntax is hard to structure
- Users prefer native StarRocks syntax
- Edge cases not covered by structured format. e.g. initial created `partitions`.

**Implementation**: Adapters parse strings into internal structures when needed.

### 3. Raw Column Names (Convenience)

**Principle**: Support unquoted column identifiers whenever possible.

**Why**: More natural and concise syntax.

**Example**:

```sql
-- ✅ Preferred: Raw column names
partitioned_by dt
clustered_by (dt, id)

-- ❌ Avoid: Quoted strings (unless necessary)
partitioned_by "dt"
clustered_by ("dt", "id")
```

### 4. Flexible Column Syntax (Robustness)

**Principle**: Support multiple formats for column specifications:

- Single column: `col1`
- Tuple of columns: `(col1, col2, col3)`
- String with comma-separated names: `"col1, col2, col3"`

**Why**: Accommodates different user preferences and source formats.

**Example**:

```sql
-- All these are valid:
order_by id
order_by (id, dt)
order_by "id, dt"
```

### Summary Table

| Principle | Preference | Fallback | Reason |
|-----------|------------|----------|--------|
| **Data Type** | Structured expressions | Strings | Type safety, validation |
| **Column Names** | Raw identifiers | Quoted strings | Conciseness |
| **Multiple Columns** | Tuple `(a, b)` | String `"a, b"` or Array | Flexibility |
| **Complex Syntax** | Key=Value pairs | Native SQL string | Clarity vs. Power |

## Parameter Names and Usage

This section describes the mapping between model parameters and physical properties.

### 1. Table Type / Key Specification

**Model parameter**: `primary_key`

- Standard SQLMesh parameter for primary key
- Example: `primary_key (id, customer_id)`

**Physical properties**: `primary_key`, `duplicate_key`, `aggregate_key`, `unique_key`

- More flexible - supports all StarRocks table types
- **Rule**: Use **either** model parameter **or** physical property, not both
- Example:

  ```sql
  physical_properties (
    duplicate_key = (id, dt),  -- DUPLICATE KEY table
    -- OR aggregate_key = (id),  -- AGGREGATE KEY table
    -- OR unique_key = (id)      -- UNIQUE KEY table
  )
  ```

### 2. Partitioning

**Model parameter**: `partitioned_by`

- Standard parameter for partition columns
- **Limitation**: Only single column per expression (validator restriction)
- Example: `partitioned_by dt` or `partitioned_by date_trunc('day', dt)`

**Physical properties**: `partition_by` or `partitioned_by` (aliases)

- **More powerful**: Supports multiple columns in RANGE/LIST
- Bypasses validator - allows `RANGE(col1, col2)`
- Example:

  ```sql
  physical_properties (
    partition_by = RANGE(dt, region),  -- Multiple columns OK!
    partitions = (
      'PARTITION p1 VALUES LESS THAN ("2024-01-01", "US")',
      'PARTITION p2 VALUES LESS THAN ("2024-01-01", "EU")'
    )
  )
  ```

**Physical property**: `partitions`

- Defines initial partition specifications
- List/Tuple of partition definition strings
- Example: `partitions = ('PARTITION p1 VALUES LESS THAN ("2024-01-01")', ...)`

### 3. Distribution

**Physical property only**: `distributed_by`

- No model parameter equivalent
- Required for StarRocks table creation
- Example:

  ```sql
  physical_properties (
    distributed_by = (kind='HASH', expressions=id, buckets=10)
  )
  ```

### 4. Ordering

**Model parameter**: `clustered_by`

- Standard parameter for clustering/ordering columns
- Example: `clustered_by (dt, id)`

**Physical property**: `order_by`

- Alias for `clustered_by` in StarRocks context
- Example: `physical_properties (order_by = (dt, id))`

## Supported property structure and examples

### Partition

#### partition method

parameter: `partitioned_by`

- `RANGE(col1, col2)`
- `LIST(col1, col2)`
- `(col1, col2, col3)`
- `date_trunc('day', col1)`
- `(date_trunc('day', col1), col2)`
- `(my_date_func(col1, "param1"), col2)`
- Or a **string** containing one above item, but can be both with outer parenthesis or not.

parameter: `parameters`

List/Tuple of following items, separated by commas. Each item is a string:

- `'START ("2019-01-01") END ("2021-01-01") EVERY (INTERVAL 1 YEAR)'`
- `'PARTITION p1 VALUES LESS THAN ("2020-01-31")'`

### Distribution

parameter: `distributed_by`

- A **tuple** of folowing **EQ** items (recommended)
  - `kind = HASH` or `kind = "HASH"`.
  - `expressions = (col1, col2, col3)`, alias: `columns`). (optional)
    - `"col1, col2, col3"` is also acceptable.
  - `buckets = 10`, alias `bucket`, `bucket_num`. (optional)
- Or a **string**, such as:
  - `"HASH(id) BUCKETS 10"`
  - `"RANDOM"`

### Order by

parameter: `order_by`

- `col1`
- `(col1, col2, col3)`
- Or a **string** containing one above item, and can be both with outer parenthesis or not.

### Other Properties

parameter: `properties`

`key = value` pairs except `partitioned_by`, `distributed_by`, `order_by`, and other pre-defined properties. Such as:

- `storage_format = "DEFAULT"`
- `compression = "SNAPPY"`
- `replication_num = 3`
