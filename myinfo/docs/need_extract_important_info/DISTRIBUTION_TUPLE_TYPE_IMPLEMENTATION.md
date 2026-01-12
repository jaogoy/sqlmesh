# DistributionTupleType Implementation Summary

## Overview

Successfully implemented a complete structured tuple validation system based on the "通用基类 + 具体实现" pattern, with:

1. **EqType** - Validates `key=value` expressions
2. **Field** - Declarative field specification
3. **StructuredTupleType** - Generic base class for typed tuples
4. **DistributionTupleType** - Concrete implementation for StarRocks distribution
5. **FuncType** - Validates function expressions (RANGE, LIST, etc.)

## Architecture

```
┌─────────────────────────────────────┐
│         Base Types Layer            │
├─────────────────────────────────────┤
│ EqType - Validates exp.EQ(a, b)     │
│ FuncType - Validates exp.Func       │
│ ExpressionType - Any expression     │
└─────────────────────────────────────┘
                ↓
┌─────────────────────────────────────┐
│      Infrastructure Layer           │
├─────────────────────────────────────┤
│ Field - Field specification         │
│ StructuredTupleType - Base class    │
└─────────────────────────────────────┘
                ↓
┌─────────────────────────────────────┐
│       Business Logic Layer          │
├─────────────────────────────────────┤
│ DistributionTupleType               │
│   FIELDS = {                        │
│     "kind": Field(...),             │
│     "columns": Field(...),          │
│     "buckets": Field(...)           │
│   }                                 │
└─────────────────────────────────────┘
```

## Key Components

### 1. EqType

Validates and extracts `key=value` pairs from `exp.EQ` expressions.

**Input**: `exp.EQ(left, right)` or `"key=value"` string

**Output**: `(key_name: str, value_expr: Any)` tuple

**Example**:
```python
EqType().validate(parse_one("kind='HASH'"))
# Returns: ('kind', Literal.string('HASH'))
```

### 2. Field Class

Declarative field specification with:
- Type validation
- Required/optional flag
- Alias support
- Documentation

**Definition**:
```python
class Field:
    def __init__(
        self,
        type: DeclarativeType,
        required: bool = False,
        aliases: Optional[List[str]] = None,
        doc: Optional[str] = None
    )
```

**Example**:
```python
Field(
    type=EnumType(["HASH", "RANDOM"], normalized_type="str"),
    required=True,
    aliases=["distribution_type"],
    doc="Distribution kind: HASH or RANDOM"
)
```

### 3. StructuredTupleType

Generic base class for validating structured tuples with typed fields.

**Subclass Pattern**:
```python
class MyTupleType(StructuredTupleType):
    FIELDS = {
        "field_name": Field(type=SomeType(), required=True, aliases=["alt_name"]),
        # ... more fields
    }
```

**Validation Process**:
1. Parse tuple into `key=value` pairs (`exp.EQ`)
2. Match keys against FIELDS (including aliases)
3. Validate each field value with specified type
4. Check required fields are present
5. Return dict with canonical field names

**Output**: `Dict[str, Any]` with normalized values

### 4. DistributionTupleType

Concrete implementation for StarRocks distribution specification.

**Field Specification**:
```python
FIELDS = {
    "kind": Field(
        type=EnumType(["HASH", "RANDOM"], normalized_type="str"),
        required=True,
        aliases=[],
        doc="Distribution type: HASH or RANDOM"
    ),
    "columns": Field(
        type=SequenceOf(ColumnType(), IdentifierType(normalized_type="column")),
        required=False,
        aliases=["expressions"],
        doc="Columns for HASH distribution"
    ),
    "buckets": Field(
        type=AnyOf(LiteralType(), StringType()),
        required=False,
        aliases=["bucket", "bucket_num"],
        doc="Number of buckets for HASH distribution"
    )
}
```

**Supported Inputs**:
```python
# HASH distribution with all fields
(kind='HASH', columns=(id, dt), buckets=10)

# Using aliases
(kind='HASH', expressions=(id, dt), bucket_num=10)
(kind='HASH', columns=(id), bucket=10)

# RANDOM distribution
(kind='RANDOM')
(kind='RANDOM', buckets=10)
```

**Output Structure**:
```python
{
    'kind': 'HASH',                        # str
    'columns': [exp.Column('id'), ...],    # List[exp.Column]
    'buckets': exp.Literal.number(10)      # exp.Literal
}
```

### 5. FuncType

Validates function expressions (both built-in and custom).

**Accepts**:
- `exp.Func` - Built-in functions (date_trunc, CAST, etc.)
- `exp.Anonymous` - Custom/dialect functions (RANGE, LIST, etc.)
- String that can be parsed as function call

**Examples**:
```python
FuncType().validate("RANGE(col1, col2)")     # → exp.Anonymous
FuncType().validate("date_trunc('day', col1)")  # → exp.Func
FuncType().validate("LIST(region, status)")    # → exp.Anonymous
```

## Alias Support

All field aliases are automatically resolved to canonical names:

| Canonical Name | Aliases |
|----------------|---------|
| `columns` | `expressions` |
| `buckets` | `bucket`, `bucket_num` |

**Example**:
```python
# Input with aliases
(kind='HASH', expressions=(id, dt), bucket_num=10)

# Normalized output (canonical names)
{
    'kind': 'HASH',
    'columns': [exp.Column('id'), exp.Column('dt')],
    'buckets': exp.Literal.number(10)
}
```

## Usage Examples

### Basic Usage

```python
from sqlmesh.core.engine_adapter.starrocks import DistributionTupleType

dist_type = DistributionTupleType()

# Validate and normalize
value = "(kind='HASH', columns=(id, dt), buckets=10)"
validated = dist_type.validate(value)
normalized = dist_type.normalize(validated)

# Access fields
if normalized['kind'] == 'HASH':
    columns = normalized.get('columns', [])
    buckets = normalized.get('buckets')
    # Process HASH distribution...
```

### Integration with DistributionSpec

```python
from sqlmesh.core.engine_adapter.starrocks import PROPERTY_INPUT_SPEC

dist_spec = PROPERTY_INPUT_SPEC["distributed_by"]

# Accepts multiple formats via AnyOf
values = [
    "(kind='HASH', columns=(id, dt), buckets=10)",  # DistributionTupleType
    "(id, dt)",                                      # SequenceOf(ColumnType)
    "HASH"                                           # StringType
]

for value in values:
    validated = dist_spec.validate(value)
    normalized = dist_spec.normalize(validated)
    # Handle different output types (dict, list, or string)
```

## Test Results

All tests passing ✅:

### Basic Functionality
- ✅ HASH with columns and buckets
- ✅ HASH with expressions alias
- ✅ HASH with bucket alias
- ✅ RANDOM only
- ✅ RANDOM with buckets

### Alias Resolution
- ✅ `columns` canonical name
- ✅ `expressions` alias → `columns`
- ✅ `buckets` canonical name
- ✅ `bucket` alias → `buckets`
- ✅ `bucket_num` alias → `buckets`
- ✅ Mixed aliases

### Required Fields
- ✅ Missing `kind` rejected
- ✅ Only `kind` accepted
- ✅ `kind` + `columns` accepted
- ✅ Empty tuple rejected

### Dict Access
- ✅ Direct field access via `normalized['field_name']`
- ✅ Safe access via `normalized.get('field_name')`
- ✅ Conditional logic based on field values

## Benefits

### 1. Type Safety
- **Precise validation**: Each field has its own type specification
- **Early error detection**: Invalid structures rejected at validation phase
- **Strong typing**: SQLGlot Expression types ensure SQL generation correctness

### 2. Flexibility
- **Alias support**: Multiple field names for user convenience
- **Optional fields**: Some fields only required for certain configurations
- **Extensible**: Easy to add new fields or validation rules

### 3. Maintainability
- **Declarative**: FIELDS dict is self-documenting
- **Centralized**: All validation rules in one place
- **Reusable**: StructuredTupleType can be used for other properties

### 4. User Experience
- **Intuitive**: Dict output with clear field names
- **Flexible input**: Accepts multiple syntaxes via aliases
- **Clear errors**: Can provide specific field-level error messages

## Type Specification Updates

### Renamed (removed Type suffix)

```python
# Before
TableKeySpecType = SequenceOf(...)
PartitionedBySpecType = SequenceOf(...)
PartitionsSpecType = SequenceOf(...)
DistributionSpecType = AnyOf(...)
OrderBySpecType = SequenceOf(...)

# After
TableKeySpec = SequenceOf(...)
PartitionSpec = SequenceOf(...)
PartitionsSpec = SequenceOf(...)
DistributionSpec = AnyOf(...)
OrderBySpec = SequenceOf(...)
```

### Enhanced

```python
# PartitionSpec now uses FuncType for RANGE/LIST
PartitionSpec = SequenceOf(
    ColumnType(),
    IdentifierType(normalized_type="column"),
    FuncType(),  # ✅ NEW! Handles RANGE(), LIST(), etc.
    allow_single=True
)

# DistributionSpec now uses DistributionTupleType
DistributionSpec = AnyOf(
    DistributionTupleType(),  # ✅ NEW! Structured tuple validation
    SequenceOf(ColumnType(), IdentifierType(normalized_type="column"), allow_single=True),
    StringType()
)
```

## Future Enhancements

Potential improvements:

1. **Conditional Validation**
   - `required_when` parameter for Field
   - Example: `columns` required when `kind='HASH'`

2. **Value Constraints**
   - Min/max for numeric fields
   - Pattern matching for strings

3. **Better Error Messages**
   - Field-level validation errors
   - Suggestion for common mistakes

4. **Type Inference**
   - Auto-detect field types from values
   - Generate TypedDict hints

5. **More StructuredTupleTypes**
   - PartitionTupleType for complex partition specs
   - IndexTupleType for index definitions

## Conclusion

The DistributionTupleType implementation provides:

✅ **Robust validation** with field-based type checking
✅ **Flexible input** with alias support
✅ **Clean output** with structured dict format  
✅ **Maintainable code** with declarative FIELDS specification
✅ **Extensible design** for future enhancements

The implementation follows best practices:
- Separation of concerns (validation vs normalization)
- Composition over inheritance (Field + StructuredTupleType)
- Declarative configuration (FIELDS dict)
- Type safety (SQLGlot expressions)

All tests passing, ready for production use! 🎉
