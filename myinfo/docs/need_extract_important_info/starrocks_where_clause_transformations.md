# StarRocks WHERE Clause Transformations

## Overview

StarRocks has specific limitations on WHERE clause syntax in DELETE statements, particularly for non-PRIMARY KEY tables (DUPLICATE, UNIQUE, and AGGREGATE KEY tables). To ensure compatibility across all table types, the StarRocks adapter implements automatic WHERE clause transformations.

## Transformations

### 1. BETWEEN to Comparison Conversion

**Issue**: BETWEEN is not supported in DELETE WHERE clauses for non-PRIMARY KEY tables.

**Transformation**: `col BETWEEN low AND high` → `col >= low AND col <= high`

**Implementation**: [`_where_clause_convert_between_to_comparison()`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py#L1911-L1946)

**Example**:
```sql
-- Input
DELETE FROM table WHERE dt BETWEEN '2024-01-01' AND '2024-12-31'

-- Transformed
DELETE FROM table WHERE dt >= '2024-01-01' AND dt <= '2024-12-31'
```

### 2. Boolean Literal Removal

**Issue**: Boolean literals (TRUE/FALSE) are not supported in WHERE clauses.

**Transformations**:
- `condition AND TRUE` → `condition`
- `TRUE AND condition` → `condition`
- `condition OR FALSE` → `condition`
- `FALSE OR condition` → `condition`
- `WHERE TRUE` → `1=1` (though TRUNCATE TABLE is used instead)
- `WHERE FALSE` → `1=0`

**Implementation**: [`_where_clause_remove_boolean_literals()`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py#L1856-L1909)

**Example**:
```sql
-- Input
DELETE FROM table WHERE id > 100 AND TRUE

-- Transformed
DELETE FROM table WHERE id > 100
```

## Implementation Details

### Why Use `copy()` for Expression Nodes?

When transforming BETWEEN expressions, we create two new comparison nodes (GTE and LTE) that both reference the same column. Without copying, this would create a graph structure where one node has multiple parents, violating the tree structure requirement of ASTs.

```python
# ❌ WRONG: Shared reference causes AST corruption
gte = exp.GTE(this=column, expression=low)      # column referenced here
lte = exp.LTE(this=column, expression=high)     # column referenced again - PROBLEM!

# ✅ CORRECT: Independent copies maintain tree structure
gte = exp.GTE(this=column.copy(), expression=low.copy())
lte = exp.LTE(this=column.copy(), expression=high.copy())
```

**Key Points**:
1. SQLGlot Expression objects are **mutable**
2. AST nodes must form a **tree** (one parent per node), not a graph
3. The outer `transform(copy=True)` ensures the input isn't modified
4. Inner `copy()` calls ensure new nodes don't share child references

### Conservative Application

These transformations are applied to **all DELETE statements** because:
1. Table type cannot be easily determined at DELETE execution time
2. The transformations are semantically equivalent and safe for all table types
3. PRIMARY KEY tables work correctly with the transformed syntax
4. Non-PRIMARY KEY tables **require** the transformations

## Testing

Comprehensive tests are in [`test_starrocks.py::TestWhereClauseTransformations`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py#L214-L446)

### Test Coverage

**BETWEEN Conversion**:
- Simple BETWEEN with dates
- BETWEEN with numeric values
- BETWEEN combined with other conditions
- Multiple BETWEEN in one clause
- Nested BETWEEN in complex expressions

**Boolean Literal Removal**:
- AND TRUE (both orders)
- OR FALSE (both orders)
- Standalone TRUE/FALSE
- Combined with BETWEEN transformations
- Nested boolean expressions

### Running Tests

```bash
# Run all WHERE transformation tests
pytest tests/core/engine_adapter/test_starrocks.py::TestWhereClauseTransformations -v

# Run specific test
pytest tests/core/engine_adapter/test_starrocks.py::TestWhereClauseTransformations::test_delete_with_between_simple -v
```

## References

- StarRocks Documentation: DELETE Statement Limitations
- Implementation: [`starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/sqlmesh/core/engine_adapter/starrocks.py)
- Tests: [`test_starrocks.py`](file:///Users/lijiao/resources/git-open-source/etl/sqlmesh/tests/core/engine_adapter/test_starrocks.py)
