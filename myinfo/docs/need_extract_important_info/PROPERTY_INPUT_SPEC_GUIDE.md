# PROPERTY_INPUT_SPEC Complete Documentation

## Overview

The `PROPERTY_INPUT_SPEC` defines comprehensive validation and normalization rules for StarRocks properties using the DeclarativeType system. This specification handles all property syntaxes found in the test models.

## Architecture

### DeclarativeType System

The type system follows a two-phase design:

1. **Validation Phase**: Type checking - determines if input conforms to expected type
2. **Normalization Phase**: Type conversion - transforms validated input to target output format

### Base Types

| Type | Accepts | Output |
|------|---------|--------|
| `StringType` | Python string | Python string |
| `LiteralType` | exp.Literal, parsable string | exp.Literal |
| `IdentifierType` | exp.Identifier, parsable string | exp.Identifier/Column/Literal (configurable) |
| `ColumnType` | exp.Column, parsable string | exp.Column/Identifier/Literal (configurable) |
| `ExpressionType` | Any exp.Expression, parsable string | exp.Expression |
| `EnumType` | Predefined values | String/Identifier/Literal/Column (configurable) |

### Composite Types

| Type | Description | Example |
|------|-------------|---------|
| `AnyOf` | Union type - accepts first matching sub-type | `AnyOf(ColumnType(), StringType())` |
| `SequenceOf` | List type - validates each element | `SequenceOf(ColumnType(), allow_single=True)` |

## Supported Properties

### 1. Partition Properties

#### `partitioned_by` / `partition_by`

**Type**: `PartitionSpecType`

**Accepts**:
- Single column: `col1`
- Multiple columns: `(col1, col2)`
- RANGE with single column: `RANGE(col1)`
- RANGE with multiple columns: `RANGE(col1, col2)`
- LIST with multiple columns: `LIST(region, status)`
- Expression partition: `date_trunc('day', col1)`
- Mixed: `(date_trunc('day', col1), col2)`

**Output**: List of `exp.Column` or `exp.Expression`

**Examples**:
```python
# Single column
partition_by = col1

# Multiple columns
partition_by = (col1, col2)

# RANGE partition
partition_by = RANGE(col1, col2)

# LIST partition
partition_by = LIST(region, status)

# Expression
partition_by = date_trunc('day', created_at)
```

#### `partitions`

**Type**: `PartitionsSpecType`

**Accepts**:
- Single partition definition (string): `'PARTITION p1 VALUES LESS THAN ("2024-01-01")'`
- Multiple partition definitions (tuple/list): `('PARTITION p1 ...', 'PARTITION p2 ...')`

**Output**: List of strings

**Examples**:
```python
# Single partition
partitions = 'PARTITION p1 VALUES LESS THAN ("2024-01-01")'

# Multiple partitions
partitions = (
    'PARTITION p1 VALUES LESS THAN ("2024-01-01", "US")',
    'PARTITION p2 VALUES LESS THAN ("2024-06-01", "US")',
    'PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE)'
)
```

### 2. Distribution Property

#### `distributed_by`

**Type**: `DistributionSpecType`

**Accepts**:
- Single column: `id`
- Multiple columns: `(id, dt)`
- Structured tuple (HASH): `(kind='HASH', columns=(id, dt), bucket_num=10)`
- RANDOM: `(kind='RANDOM')`
- String format: `"(kind='HASH', columns=(id, dt), bucket_num=10)"`
- Any expression

**Output**: `exp.Expression` or list of `exp.Column`

**Examples**:
```python
# Single column
distributed_by = id

# Multiple columns
distributed_by = (id, dt)

# HASH distribution with structured syntax
distributed_by = (kind='HASH', columns=(id, dt), bucket_num=10)

# RANDOM distribution
distributed_by = (kind='RANDOM')

# String format
distributed_by = "(kind='HASH', columns=(id, dt), bucket_num=10)"
```

### 3. Ordering Property

#### `order_by`

**Type**: `OrderBySpecType`

**Accepts**:
- Single column: `dt`
- Multiple columns: `(dt, id, status)`
- Expressions with ASC/DESC: `col1 ASC, col2 DESC`

**Output**: List of `exp.Column` or `exp.Expression`

**Examples**:
```python
# Single column
order_by = dt

# Multiple columns
order_by = (dt, id, status)

# With ordering direction
order_by = (dt DESC, id ASC)
```

### 4. Table Key Properties

#### `primary_key`, `duplicate_key`, `unique_key`, `aggregate_key`

**Type**: `TableKeySpecType`

**Accepts**:
- Single column: `id`
- Multiple columns: `(id, dt)`
- Tuple: `(id, customer_id)`
- Expression: `my_hash_func(id)`

**Output**: List of `exp.Column` or `exp.Expression`

**Examples**:
```python
# Single column
duplicate_key = dt

# Multiple columns
duplicate_key = (dt, id)

# Tuple format
primary_key = (id, customer_id)

# Expression
aggregate_key = (id, SUM(amount))
```

### 5. Generic Properties

#### Common StarRocks Properties

All generic properties use `GenericPropertyType` which accepts:
- exp.Literal
- Python string
- exp.Identifier
- exp.Expression

**Supported Properties**:

| Property | Description | Example |
|----------|-------------|---------|
| `replication_num` | Replication factor | `replication_num = 3` |
| `storage_medium` | Storage medium type | `storage_medium = "SSD"` |
| `storage_cooldown_time` | Cooldown timestamp | `storage_cooldown_time = "2024-12-31 23:59:59"` |
| `storage_cooldown_ttl` | Cooldown TTL | `storage_cooldown_ttl = "30d"` |
| `enable_persistent_index` | Enable persistent index | `enable_persistent_index = "true"` |
| `compression` | Compression type | `compression = "LZ4"` |
| `storage_format` | Storage format | `storage_format = "DEFAULT"` |
| `bloom_filter_columns` | Bloom filter columns | `bloom_filter_columns = "(id, name)"` |
| `colocate_with` | Colocate group | `colocate_with = "group1"` |
| `dynamic_partition` | Dynamic partition settings | `dynamic_partition = "..."` |
| `in_memory` | In-memory flag | `in_memory = "false"` |
| `foreign_key_constraints` | Foreign key constraints | `foreign_key_constraints = "..."` |
| `unique_constraints` | Unique constraints | `unique_constraints = "..."` |

**Examples**:
```python
# Numeric property
replication_num = 3

# String property
storage_medium = "SSD"

# Timestamp property
storage_cooldown_time = "2024-12-31 23:59:59"

# Boolean-like property
enable_persistent_index = "true"
```

## Complete Example

```python
MODEL (
  name mytest.comprehensive_example,
  kind FULL,
  dialect starrocks,
  
  physical_properties (
    -- Table type
    duplicate_key = (dt, region, id),
    
    -- Partitioning
    partition_by = RANGE(dt, region),
    partitions = (
      'PARTITION p1 VALUES LESS THAN ("2024-01-01", "US")',
      'PARTITION p2 VALUES LESS THAN ("2024-06-01", "US")',
      'PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE)'
    ),
    
    -- Distribution
    distributed_by = (kind='HASH', columns=(id, region), bucket_num=16),
    
    -- Ordering
    order_by = (dt, region, id),
    
    -- Generic properties
    replication_num = 3,
    storage_medium = "SSD",
    storage_cooldown_time = "2024-12-31 23:59:59",
    enable_persistent_index = "true"
  ),
  
  columns (
    id INT,
    dt DATE,
    region STRING,
    status STRING,
    amount DECIMAL(18,2)
  )
);
```

## Implementation Details

### Type Hierarchy

```
DeclarativeType (Base)
├── StringType
├── LiteralType
├── IdentifierType
├── ColumnType
├── ExpressionType
├── EnumType
├── AnyOf (Combinator)
└── SequenceOf (Combinator)
```

### Validation Flow

1. **Input**: Raw property value (string, expression, tuple, etc.)
2. **Validation**: Type checker determines if input is valid
   - Returns validated intermediate value if valid
   - Returns None if invalid
3. **Normalization**: Converts validated value to target output format
   - Transforms to SQLGlot Expression types
   - Ensures consistent structure for SQL generation
4. **Output**: Normalized value ready for SQL generation

### Key Design Decisions

1. **Flexible Input**: Accepts multiple syntax variations for user convenience
2. **Strict Output**: Normalizes to consistent SQLGlot Expression types
3. **Composability**: Uses combinator types (AnyOf, SequenceOf) for complex patterns
4. **Type Safety**: Clear separation between validation and normalization
5. **Extensibility**: Easy to add new properties or types

## Testing

All syntax variations are tested in:
- `myinfo/tests/test_models_syntax_parse/` - Model definitions with various syntaxes
- `mytest/test_property_input_spec.py` - Unit tests for PROPERTY_INPUT_SPEC

Run tests:
```bash
python mytest/test_property_input_spec.py
```

## Future Enhancements

Potential improvements:
1. Add support for more complex distribution syntaxes
2. Support for computed columns in table keys
3. Enhanced validation messages with suggestions
4. Support for more StarRocks-specific properties
5. Auto-completion support for property names and values
