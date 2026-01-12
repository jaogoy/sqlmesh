# SQLMesh StarRocks Model Properties Test Suite

This directory contains tests to understand how SQLMesh parses model properties and transforms them for StarRocksEngineAdapter.

## Files Structure

### Model Files

1. **`models/starrocks_comprehensive.sql`** - Comprehensive model with all property types:
   - Partitioning: `partitioned_by (event_date)`
   - Clustering: `clustered_by (customer_id, region)`
   - Distribution: `distributed_by = (kind='HASH', expressions=customer_id, buckets=10)`
   - Partition definitions: `partitions = (...)`
   - Table properties: `replication_num`, `storage_medium`, etc.
   - Storage format: `storage_format parquet`

2. **`models/starrocks_complex_partition.sql`** - Model with complex partition expressions:
   - Complex partitioning: `partitioned_by (date_trunc('day', event_date), customer_id)`
   - Tests expression support in partitioned_by

### Test Files

| File | Purpose | When to Use | Key Features |
|------|---------|-------------|--------------|
| **test_print_params.py** ⭐ | Print actual parameters passed to adapter | **Primary test for development** | • No database connection needed<br>• Shows exact parameter structure<br>• Detailed distributed_by breakdown<br>• No mocking required |
| test_parse_model.py | Show how model attributes are stored internally | Understanding SQLMesh internals | • Shows model.__dict__<br>• Raw attribute display<br>• Good for debugging parsing |
| test_adapter_parameters.py | Full flow from model to SQL generation | Understanding complete pipeline | • Uses mocking<br>• Shows SQL generation<br>• More complex setup |
| test_direct_adapter.py | Monkey-patch adapter to intercept calls | Real plan execution testing | • Requires database connection<br>• Tests actual plan flow<br>• May timeout without DB |

### Config

- **`config.yaml`** - StarRocks connection configuration

## Key Findings

### 1. Why partitioned_by is List but distributed_by is Tuple?

**Answer**: Different parsing paths in SQLMesh dialect.py:

```python
# Line 635 in dialect.py
elif key == "partitioned_by":
    partitioned_by = self._parse_partitioned_by()  # Returns PartitionedByProperty
    if isinstance(partitioned_by.this, exp.Schema):
        value = exp.tuple_(*partitioned_by.this.expressions)  # Convert to Tuple
    else:
        value = partitioned_by.this  # Keep original
```

- `partitioned_by` calls `_parse_partitioned_by()` which returns `exp.PartitionedByProperty`
- The `.this` of PartitionedByProperty is `exp.Schema(expressions=[...])`
- `exp.Schema.expressions` is a **list**
- This list is extracted and stored as `model.partitioned_by_`

```python
# Line 639 in dialect.py
else:
    value = self._parse_bracket(self._parse_field(any_token=True))
```

- `distributed_by` (and other properties) uses generic parsing: `_parse_bracket(_parse_field())`
- `_parse_field()` parses `(kind='HASH', expressions=customer_id, buckets=10)` as a **Tuple**
- Each item in the tuple is an **EQ expression** (e.g., `kind = 'HASH'`)

**Summary**:
- `partitioned_by`: Special parser → PartitionedByProperty → Schema → **List of expressions**
- `distributed_by`: Generic parser → Paren/Tuple → **Tuple of EQ expressions**

### 2. Why kind/expressions/buckets parsed as Column?

**Answer**: Generic field parsing treats identifiers as columns by default.

In `distributed_by = (kind='HASH', expressions=customer_id, buckets=10)`:

```python
# _parse_field() in SQLGlot parser
kind='HASH'  →  EQ(Column('kind'), Literal('HASH'))
expressions=customer_id  →  EQ(Column('expressions'), Column('customer_id'))
buckets=10  →  EQ(Column('buckets'), Literal(10))
```

The parser sees:
- Left side of `=`: bare identifier → interpreted as **Column**
- Right side of `=`:
  - String with quotes → **Literal**
  - Number → **Literal**
  - Bare identifier → **Column** (could be a column reference)

This is **not a bug** - it's the generic SQL parser behavior. The Doris/StarRocks adapter must **interpret** these EQ expressions correctly:

```python
# In adapter's _build_table_properties_exp()
for expr in distributed_by.expressions:
    if isinstance(expr, exp.EQ):
        key = str(expr.this.this).strip('"')  # Extract 'kind', 'expressions', 'buckets'
        value = expr.expression  # Get the right side
        distributed_info[key] = value
```

### 3. How does SQLMesh know INT is a type, not a Column?

**Answer**: Context-specific parsing in the `columns` clause.

```python
# Line 598 in dialect.py
elif key == "columns":
    value = self._parse_schema()
```

When parsing `columns (order_id INT, customer_id INT)`:
- `_parse_schema()` is called (from SQLGlot's standard parser)
- This method **expects** column definitions in the form: `name type`
- It creates `exp.ColumnDef(this=Identifier('order_id'), kind=DataType(this=INT))`

The parser knows the **context**:
- In `columns (...)` → expect column definitions → parse as **DataType**
- In `distributed_by (...)` → generic expression → parse as **Column** or **Literal**
- In `partitioned_by (...)` → expect column/expression list → parse as **Column/Function**

**Key insight**: SQLMesh/SQLGlot parsers are **context-aware** - they use different parsing strategies based on which clause they're in.

### 4. Property Transformation Flow

```
MODEL Definition (SQL)
    ↓
SQLMesh Dialect Parser (dialect.py)
    ├─ partitioned_by → _parse_partitioned_by() → List[Expression]
    ├─ columns → _parse_schema() → Dict[str, DataType]
    └─ distributed_by → _parse_field() → Tuple[EQ, ...]
    ↓
Model Object (meta.py)
    ├─ model.partitioned_by_: List[exp.Column | exp.Function]
    ├─ model.columns_to_types: Dict[str, exp.DataType]
    └─ model.physical_properties: Dict[str, exp.Expression]
    ↓
Adapter Method Call
    adapter._create_table_from_columns(
        partitioned_by=List[exp.Expression],
        table_properties=Dict[str, exp.Expression],
        target_columns_to_types=Dict[str, exp.DataType]
    )
    ↓
Adapter Implementation
    _build_table_properties_exp() parses table_properties dict:
        - Extract distributed_by Tuple
        - Parse EQ expressions to get kind/expressions/buckets
        - Build exp.DistributedByProperty
    ↓
SQL Generation
    StarRocks.Generator.sql() → CREATE TABLE ... DISTRIBUTED BY ...
```

## Running Tests

```bash
# Primary test - shows exact parameters
cd /path/to/sqlmesh
python mytest/test_model/test_print_params.py

# See model internal structure
python mytest/test_model/test_parse_model.py

# Full pipeline test (with mocking)
python mytest/test_model/test_adapter_parameters.py

# Real plan test (needs StarRocks running)
python mytest/test_model/test_direct_adapter.py
```

## Important Observations

### distributed_by Structure

```python
# Model definition:
distributed_by = (kind='HASH', expressions=customer_id, buckets=10)

# Parsed as:
exp.Tuple([
    exp.EQ(this=Column('kind'), expression=Literal('HASH')),
    exp.EQ(this=Column('expressions'), expression=Column('customer_id')),
    exp.EQ(this=Column('buckets'), expression=Literal(10))
])

# Adapter must extract:
distributed_info = {
    'kind': 'HASH',
    'expressions': [Column('customer_id')],  # or list of columns
    'buckets': 10
}
```

### Complex Partition Expressions

Fully supported! Examples:

```sql
-- Simple column
partitioned_by (event_date)
→ [Column('event_date')]

-- Function expression
partitioned_by (date_trunc('day', event_date))
→ [TimestampTrunc(Column('event_date'), unit='DAY')]

-- Multiple expressions
partitioned_by (date_trunc('day', event_date), customer_id)
→ [TimestampTrunc(...), Column('customer_id')]
```

## Next Steps for Implementation

1. **Implement StarRocksEngineAdapter._build_table_properties_exp()**:
   - Parse `distributed_by` Tuple into `exp.DistributedByProperty`
   - Handle `partitions` for RANGE/LIST partitioning
   - Process other properties (replication_num, etc.)

2. **Reference Doris Implementation**: See `sqlmesh/core/engine_adapter/doris.py` lines 621-1089

3. **Test with actual StarRocks database** using the model files provided
