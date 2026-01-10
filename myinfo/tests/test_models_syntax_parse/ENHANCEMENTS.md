# Test Script Enhancements - physical_properties Support

## Summary of Changes

Enhanced `test_parse_model_syntax.py` to analyze and display partition-related properties from `physical_properties` block.

## What Was Added

### 1. New Function: `analyze_physical_partition_properties()`

**Purpose**: Parse and analyze partition properties in `physical_properties`

**Features**:
- Detects partition-related keys: `partition_by`, `partitioned_by`, `partitions`
- Analyzes expression structure recursively
- Handles multiple data types:
  - `exp.Expression` - Analyzes structure and shows SQL
  - `list` - Iterates and analyzes each item
  - `str` - Attempts to parse as SQL expression

**Code location**: Lines 52-116 in `test_parse_model_syntax.py`

### 2. Enhanced Output Section

**New section**: "PARTITION PROPERTIES IN physical_properties"

Shows:
- Property name (`partition_by`, `partitions`, etc.)
- Type (Anonymous, Tuple, List, etc.)
- Raw value
- Expression structure (via `analyze_expression`)
- SQL representation

### 3. New Test Models

Created models to test multi-column partition syntax via `physical_properties`:

#### test_all_partitions.sql
```sql
physical_properties (
  partition_by = RANGE(col1, col2),  -- Multi-column RANGE
  partitions = (...)
)
```

#### test_list_multi_columns.sql
```sql
physical_properties (
  partition_by = LIST(region, status),  -- Multi-column LIST
  partitions = (...)
)
```

### 4. Updated MODEL_MAP

Added shortcuts:
- `all` → test_all_partitions
- `range_multi` → test_range_multi_columns
- `list_multi` → test_list_multi_columns

## Usage Examples

### Test RANGE with multiple columns

```bash
python test_parse_model_syntax.py -m range_multi
```

**Output shows**:
```
Property: partition_by
Type: Anonymous
Raw value: RANGE(col1, col2)

Expression Analysis:
  type: Anonymous
  args:
    this: RANGE
    expressions:
      [0]: Column(col1)
      [1]: Column(col2)

SQL: RANGE(col1, col2)
```

### Test LIST with multiple columns

```bash
python test_parse_model_syntax.py -m list_multi
```

**Output shows**:
```
Property: partition_by
Type: List
Raw value: LIST(region, status)

Expression Analysis:
  type: List
  args:
    expressions:
      [0]: Column(region)
      [1]: Column(status)

SQL: LIST(region, status)
```

### Test with partition definitions

```bash
python test_parse_model_syntax.py -m all
```

**Output shows both**:
- `partition_by = RANGE(col1, col2)` - The partition columns
- `partitions = (...)` - The partition definitions (as Tuple of Literals)

## Key Insights from Testing

### 1. Expression Type Differences

| Syntax | When using partitioned_by | When using partition_by |
|--------|--------------------------|------------------------|
| `RANGE(col)` | Anonymous | Anonymous |
| `LIST(col)` | Anonymous | **List** expression |
| `(col1, col2)` | Multiple Columns | Tuple |

**Note**: SQLGlot parses `LIST(...)` differently in different contexts!

### 2. Validation Bypass Confirmed

**Model parameter** (`partitioned_by`):
```python
# validator checks each expression
for expression in expressions:
    num_cols = len(list(expression.find_all(exp.Column)))
    if num_cols > 1:
        raise ConfigError("contains multiple columns")  # ← BLOCKS RANGE(col1, col2)
```

**Physical property** (`partition_by`):
```python
# No validation! Directly passed to adapter
physical_properties = {"partition_by": Anonymous("RANGE", [Column("col1"), Column("col2")])}
# Adapter extracts and parses it without restriction
```

### 3. Adapter Processing Flow

From `starrocks.py`:
```python
# Line 626-630
if partitioned_by is None:
    partitioned_by = table_properties.pop("partition_by", None)
    # ← Gets it from physical_properties if model param is empty

# Line 635
partition_cols, partition_kind = self._parse_partition_expressions(partitioned_by)
# ← Parses RANGE(col1, col2) correctly, extracting both columns
```

## Benefits

1. **Visual confirmation** of how `physical_properties` bypasses validation
2. **Structure inspection** shows exactly how multi-column RANGE/LIST are parsed
3. **Easy testing** of different partition syntaxes
4. **Educational tool** for understanding SQLMesh's two-tier property system

## Files Modified

- ✅ `test_parse_model_syntax.py` - Added partition property analysis
- ✅ `test_all_partitions.sql` - Updated to use physical_properties
- ✅ `test_range_multi_columns.sql` - Created (already existed)
- ✅ `test_list_multi_columns.sql` - Created (new)
- ✅ `QUICK_START.md` - Updated with new models and usage

## Next Steps

To test other properties in `physical_properties`:

1. **distributed_by**: Add similar analysis for distribution properties
2. **order_by**: Analyze ordering specifications
3. **properties**: Generic key=value properties

The `analyze_physical_partition_properties()` function can be used as a template for analyzing other property types.
