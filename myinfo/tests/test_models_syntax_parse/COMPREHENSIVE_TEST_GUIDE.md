# Comprehensive Physical Properties Testing Guide

## Overview

This guide describes the comprehensive test suite for StarRocks/Doris `physical_properties` parsing, covering:
- ✅ Partitioning (partition_by, partitions)
- ✅ Distribution (distributed_by)
- ✅ Ordering (order_by)
- ✅ Table Keys (duplicate_key, unique_key, aggregate_key, primary_key)

## Test Models Reference

### 1. Partition Tests

#### Basic (Single Column)
```bash
python test_parse_model_syntax.py -m range    # RANGE(dt)
python test_parse_model_syntax.py -m list     # (col1, col2)
python test_parse_model_syntax.py -m expr     # date_trunc('day', col)
```

#### Advanced (Multi-Column via physical_properties)
```bash
python test_parse_model_syntax.py -m range_multi  # RANGE(col1, col2)
python test_parse_model_syntax.py -m list_multi   # LIST(region, status)
```

**Why needed**: Model-level `partitioned_by` rejects multi-column expressions.

---

### 2. Distribution Tests

#### test_distribution_hash.sql
```sql
physical_properties (
  distributed_by = (kind='HASH', expressions=id, buckets=10)
)
```

**Test with**:
```bash
python test_parse_model_syntax.py -m hash
```

**Output shows**:
- Type: `Tuple`
- Structure: 3 EQ expressions (kind=..., expressions=..., buckets=...)
- Each EQ contains Column and Literal/Expression

#### test_distribution_random.sql
```sql
physical_properties (
  distributed_by = (kind='RANDOM')
)
```

**Test with**:
```bash
python test_parse_model_syntax.py -m random
```

**Output shows**:
- Type: `Tuple`
- Structure: Single EQ expression (kind='RANDOM')

---

### 3. Ordering Tests

#### test_order_by.sql
```sql
physical_properties (
  order_by = (dt, id, status)
)
```

**Test with**:
```bash
python test_parse_model_syntax.py -m order
```

**Output shows**:
- Type: `Tuple`
- Structure: 3 Column expressions
- Each column is unquoted identifier

**Note**: `order_by` is StarRocks-specific alias for `clustered_by`.

---

### 4. Table Key Tests

#### test_table_keys.sql
```sql
physical_properties (
  duplicate_key = (dt, id)
)
```

**Test with**:
```bash
python test_parse_model_syntax.py -m keys
```

**Output shows**:
- Type: `Tuple`
- Structure: 2 Column expressions
- Property name: `duplicate_key`

**Supported key types**:
- `duplicate_key` - For DUPLICATE KEY tables (most common)
- `unique_key` - For UNIQUE KEY tables
- `aggregate_key` - For AGGREGATE KEY tables
- `primary_key` - For PRIMARY KEY tables (StarRocks)

---

### 5. Comprehensive Test

#### test_comprehensive_properties.sql
```sql
physical_properties (
  duplicate_key = (dt, region, id),
  partition_by = RANGE(dt, region),
  partitions = (...),
  distributed_by = (kind='HASH', expressions=(id, region), buckets=16),
  order_by = (dt, region, id),
  replication_num = 3,
  storage_medium = "SSD",
  storage_cooldown_time = "2024-12-31 23:59:59"
)
```

**Test with**:
```bash
python test_parse_model_syntax.py -m comp
```

**Output shows ALL sections**:
- PARTITION PROPERTIES
  - partition_by: RANGE(dt, region)
  - partitions: Tuple of 4 Literal strings
- DISTRIBUTION PROPERTIES
  - distributed_by: Tuple with multi-column expressions
- ORDERING PROPERTIES
  - order_by: Tuple of 3 columns
- TABLE KEY PROPERTIES
  - duplicate_key: Tuple of 3 columns
- Plus: replication_num, storage_medium, etc.

---

## Analysis Functions

### analyze_physical_partition_properties()
Analyzes: `partition_by`, `partitioned_by`, `partitions`

**Handles**:
- Anonymous expressions (RANGE/LIST)
- Tuple expressions
- Literal strings
- Lists of partitions

### analyze_physical_distribution_properties()
Analyzes: `distributed_by`

**Handles**:
- Tuple with EQ expressions
- String syntax (fallback)

### analyze_physical_ordering_properties()
Analyzes: `order_by`

**Handles**:
- Tuple of columns
- Single column
- String syntax (fallback)

### analyze_physical_key_properties()
Analyzes: `primary_key`, `duplicate_key`, `unique_key`, `aggregate_key`

**Handles**:
- Tuple of columns
- Single column

---

## Expression Types Found

### Partition Properties
| Syntax | Type | Args |
|--------|------|------|
| `RANGE(col1, col2)` | Anonymous | this='RANGE', expressions=[Column, Column] |
| `LIST(col1, col2)` | List | expressions=[Column, Column] |
| `(col1, col2)` | Tuple | expressions=[Column, Column] |

### Distribution Properties
| Syntax | Type | Structure |
|--------|------|-----------|
| `(kind='HASH', expressions=id, buckets=10)` | Tuple | expressions=[EQ, EQ, EQ] |
| `(kind='RANDOM')` | Tuple | expressions=[EQ] |

### Ordering Properties
| Syntax | Type | Structure |
|--------|------|-----------|
| `(dt, id, status)` | Tuple | expressions=[Column, Column, Column] |

### Key Properties
| Syntax | Type | Structure |
|--------|------|-----------|
| `(dt, id)` | Tuple | expressions=[Column, Column] |

---

## Output Sections

When running `test_parse_model_syntax.py -m <model>`, you'll see:

### 1. Basic Model Attributes
- name, dialect, kind, cron
- columns_to_types
- model-level partitioned_by (if any)

### 2. Physical Properties (Raw)
Lists all properties with types:
```
physical_properties:
  partition_by: RANGE(col1, col2) (type: Anonymous)
  distributed_by: (kind = 'HASH', ...) (type: Tuple)
  order_by: (dt, id) (type: Tuple)
  ...
```

### 3. PARTITION PROPERTIES Analysis
- Detailed structure of partition_by/partitions
- Expression tree breakdown
- SQL representation

### 4. DISTRIBUTION PROPERTIES Analysis
- Detailed structure of distributed_by
- EQ expressions breakdown
- Shows kind, expressions, buckets

### 5. ORDERING PROPERTIES Analysis
- Detailed structure of order_by
- Column list
- SQL representation

### 6. TABLE KEY PROPERTIES Analysis
- Shows which key type (duplicate/unique/aggregate/primary)
- Column list
- Structure breakdown

---

## Key Findings

### 1. Why physical_properties Works

```
Model Parameter:
  partitioned_by RANGE(col1, col2)
    ↓
  Validator: expression.find_all(Column) → 2 columns
    ↓
  ❌ ConfigError: "contains multiple columns"

Physical Property:
  physical_properties (partition_by = RANGE(col1, col2))
    ↓
  No validator check!
    ↓
  Adapter receives and parses correctly
    ↓
  ✅ Works!
```

### 2. Expression Type Differences

**RANGE** syntax:
- Model param: `Anonymous`
- Physical prop: `Anonymous`
- **Same parsing**

**LIST** syntax:
- Model param: `Anonymous`
- Physical prop: **`List`** ✨
- **Different parsing!**

### 3. Structured vs String Fallback

All properties support both:
- **Structured**: `(kind='HASH', expressions=id, buckets=10)`
- **String**: `"HASH(id) BUCKETS 10"`

Structured is preferred (type-safe, easier to parse).

---

## Best Practices

1. **Use structured syntax** for complex properties
   ```sql
   -- ✅ Good
   distributed_by = (kind='HASH', expressions=id, buckets=10)

   -- ❌ Avoid (harder to parse)
   distributed_by = "HASH(id) BUCKETS 10"
   ```

2. **Use physical_properties** for multi-column RANGE/LIST
   ```sql
   -- ✅ This works
   physical_properties (partition_by = RANGE(dt, region))

   -- ❌ This fails
   partitioned_by RANGE(dt, region)
   ```

3. **Test your models** with these scripts before deployment
   ```bash
   python test_parse_model_syntax.py -m your_model
   ```

---

## Quick Reference

| Property | Model Param | Physical Prop | Multi-column Support |
|----------|-------------|---------------|---------------------|
| Partition | `partitioned_by` | `partition_by` | Only in physical_prop |
| Distribution | N/A | `distributed_by` | Yes (always) |
| Ordering | `clustered_by` | `order_by` | Yes (both) |
| Table Key | `primary_key` | `duplicate_key`, etc. | Yes (both) |

**Remember**: `physical_properties` bypasses model-level validation, allowing more flexibility!
