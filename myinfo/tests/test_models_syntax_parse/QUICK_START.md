# Quick Start Guide

## Running Tests

```bash
cd myinfo/tests/test_models_syntax_parse

# Analyze all partition models
python test_parse_partitions.py

# Analyze specific model - Basic partition tests
python test_parse_model_syntax.py -m range         # Single column RANGE
python test_parse_model_syntax.py -m list          # Single column LIST
python test_parse_model_syntax.py -m expr          # Expression partition
python test_parse_model_syntax.py -m simple        # Simple column

# Multi-column partition tests
python test_parse_model_syntax.py -m all           # Multiple properties test
python test_parse_model_syntax.py -m range_multi   # RANGE with multiple columns
python test_parse_model_syntax.py -m list_multi    # LIST with multiple columns

# Distribution tests
python test_parse_model_syntax.py -m hash          # HASH distribution
python test_parse_model_syntax.py -m random        # RANDOM distribution

# Ordering tests
python test_parse_model_syntax.py -m order         # ORDER BY clause

# Table key tests
python test_parse_model_syntax.py -m keys          # DUPLICATE KEY

# Comprehensive test
python test_parse_model_syntax.py -m comp          # All properties combined
```

**Note**: Both scripts load ALL models in the `models/` directory. SQLMesh Context doesn't support loading only a single model file.

## What You'll See

### test_parse_partitions.py Output

- ✅ List of all models found
- ✅ For each model:
  - Partition expression count
  - Detailed structure (Type, SQL, Args)
- ✅ Comparison table of all partitions
- ✅ Summary statistics

### test_parse_model_syntax.py Output

- ✅ Model basic info (name, type, kind, dialect)
- ✅ Basic model attributes (name, cron, columns, etc.)
- ✅ **Complete partitioned_by structure** (via analyze_expression)
- ✅ Physical properties inspection

**Key Features**:
- Directly prints entire `partitioned_by` object structure
- Uses `analyze_expression()` and `print_structure()` for deep analysis
- Shows both full structure and simple representation

## Key Concepts

1. **One partition per model file** - SQLMesh only uses the last `partitioned_by` defined
2. **All models are always loaded** - SQLMesh Context loads all models in `models/` directory
3. **Models are parsed by SQLMesh Context** - Not regex extraction
4. **partitioned_by_ is a list** - Even for single partition: `[Column('dt')]`
5. **Expressions are SQLGlot objects** - With type, sql(), key, args
6. **Shared utilities** - Avoid code duplication

## Test Models

### Model-level partitioned_by (Standard)

| Model | Syntax | Expression Type |
|-------|--------|----------------|
| test_range_partition | `RANGE(dt)` | Anonymous |
| test_list_partition | `(col1, col2)` | Column (2x) |
| test_expression_partition | `date_trunc('day', created_at)` | DateTrunc |
| test_simple_partition | `dt` | Column |

### Physical properties partition_by (Advanced)

**Multi-column partition support**:

| Model | Syntax | Why physical_properties? |
|-------|--------|-------------------------|
| test_all_partitions | `partition_by = RANGE(col1, col2)` | Multi-column RANGE |
| test_range_multi_columns | `partition_by = RANGE(col1, col2)` | Multi-column RANGE |
| test_list_multi_columns | `partition_by = LIST(region, status)` | Multi-column LIST |

### Physical properties - Distribution (NEW)

| Model | distributed_by | Type |
|-------|---------------|------|
| test_distribution_hash | `(kind='HASH', expressions=id, buckets=10)` | Structured Tuple |
| test_distribution_random | `(kind='RANDOM')` | Simple key-value |

### Physical properties - Ordering (NEW)

| Model | order_by | Notes |
|-------|----------|-------|
| test_order_by | `(dt, id, status)` | Multi-column ordering |

### Physical properties - Table Keys (NEW)

| Model | Key Type | Syntax |
|-------|----------|--------|
| test_table_keys | duplicate_key | `(dt, id)` |

### Comprehensive Test (NEW)

| Model | Properties Tested |
|-------|------------------|
| test_comprehensive_properties | duplicate_key, partition_by (multi-col), distributed_by (multi-col), order_by, replication_num, storage_medium |

**Key insight**: These models would **fail validation** if using model-level parameters due to restrictions like "contains multiple columns" error.

## Extending Tests

To test other properties (e.g., distributed_by):

1. Create model files with the property
2. Import shared utilities:
   ```python
   from myinfo.tests.test_utils import print_expression_detailed
   from myinfo.tests.test_init_models import create_context, select_models
   ```
3. Load models and analyze properties
4. No regex extraction needed - use parsed model attributes!
