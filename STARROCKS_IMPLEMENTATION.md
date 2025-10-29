# StarRocks Engine Adapter Implementation Guide

## 📚 Part 1: Understanding Method Override Strategy

### Why Do We Need to Override Methods?

```
┌─────────────────────────────────────────────────────────────┐
│  Decision Tree: Should I Override This Method?             │
└─────────────────────────────────────────────────────────────┘

1. Does StarRocks use DIFFERENT SQL syntax than base class?
   ├─ YES → Override needed
   │  Examples:
   │  • create_schema(): Base uses "CREATE SCHEMA", StarRocks uses "CREATE DATABASE"
   │  • _create_table_from_columns(): Doris converts PRIMARY KEY → UNIQUE KEY
   │
   └─ NO → Check next question

2. Does StarRocks have DIFFERENT behavior/features?
   ├─ YES → Override needed
   │  Examples:
   │  • delete_from(): StarRocks Primary Key tables support subquery
   │  • _build_table_properties_exp(): Different table property syntax
   │
   └─ NO → Check next question

3. Can base class behavior be configured via class attributes?
   ├─ YES → Just set the attribute, NO override needed
   │  Examples:
   │  • INSERT_OVERWRITE_STRATEGY = DELETE_INSERT → Base class handles it
   │  • SUPPORTS_TRANSACTIONS = False → Base class knows not to use transactions
   │
   └─ NO → Override needed

4. Is the method implementation database-agnostic?
   └─ YES → NO override needed, use base class
      Examples:
      • fetchall(): Just returns query results
      • execute(): Just executes SQL
```

### Public Methods vs Private Methods

```python
# Public methods (no underscore prefix)
create_schema()       # ✅ Override if syntax differs
create_table()        # ❌ Don't override - this is a template method
insert_append()       # ❌ Don't override - uses strategy pattern

# Private methods (underscore prefix)
_create_schema()      # ⚠️ Usually called by public method, base class handles
_create_table_from_columns()  # ✅ Override if table creation syntax differs
_build_table_properties_exp() # ✅ Override if table properties differ
```

**Rule of Thumb**:
- Public methods: Override only if you need to change the **interface** or add **preprocessing**
- Private methods: Override when you need to change the **implementation details**

---

## 🔍 Part 2: Method-by-Method Analysis

### Method #1: `create_schema()` - ✅ MUST Override

**Base Class Implementation**:
```python
# sqlmesh/core/engine_adapter/base.py
def create_schema(self, schema_name, ...):
    return self._create_schema(
        schema_name=schema_name,
        kind="SCHEMA",  # ❌ StarRocks doesn't use SCHEMA
        ...
    )
```

**Why Override?**
- Base class generates: `CREATE SCHEMA my_database`
- StarRocks requires: `CREATE DATABASE my_database`
- Same as MySQL and Doris

**StarRocks Implementation**:
```python
def create_schema(self, schema_name, ...):
    return super()._create_schema(
        schema_name=schema_name,
        kind="DATABASE",  # ✅ Use DATABASE keyword
        ...
    )
```

**Is this a public or private method?**
- Public method (no underscore)
- But we're just changing ONE parameter (kind)
- We call the base class `_create_schema()` which does the real work

---

### Method #2: `_create_table_from_columns()` - ✅ MUST Override (This is the CORE)

**Why This is the Most Important Method**:

This is where the **PRIMARY KEY vs UNIQUE KEY** difference matters.

**Base Class Flow**:
```python
# Base class: sqlmesh/core/engine_adapter/base.py
def _create_table_from_columns(self, ..., primary_key=None, ...):
    # 1. Build column definitions: (id INT, name VARCHAR)
    columns = self._build_column_defs(...)

    # 2. Build table properties
    properties = self._build_table_properties_exp(
        primary_key=primary_key,  # ← Expects PRIMARY KEY to be handled
        ...
    )

    # 3. Generate: CREATE TABLE t (id INT, name VARCHAR) PRIMARY KEY(id) ...
    create_exp = exp.Create(...)

    # 4. Execute
    self.execute(create_exp)
```

**Doris Override** (for comparison):
```python
# Doris: sqlmesh/core/engine_adapter/doris.py
def _create_table_from_columns(self, ..., primary_key=None, ...):
    table_properties = kwargs.get("table_properties", {})

    # 🔄 CONVERT: primary_key → unique_key
    if primary_key:
        table_properties["unique_key"] = exp.Tuple(
            expressions=[exp.to_column(col) for col in primary_key]
        )

    kwargs["table_properties"] = table_properties

    # ❌ Block base class from handling primary_key
    super()._create_table_from_columns(
        primary_key=None,  # Set to None!
        **kwargs
    )
    # Result: CREATE TABLE t (...) UNIQUE KEY(id)
```

**StarRocks Implementation** (much simpler!):
```python
# StarRocks: Just use base class as-is
def _create_table_from_columns(self, ..., primary_key=None, ...):
    # ✅ No conversion needed
    super()._create_table_from_columns(
        primary_key=primary_key,  # Pass as-is
        **kwargs
    )
    # Result: CREATE TABLE t (...) PRIMARY KEY(id)
```

**Why is this a private method?**
- It's called by the public method `create_table()`
- The public method handles the overall flow (CTAS vs from columns)
- This private method handles the specific implementation for column-based creation

---

### Method #3: `_get_data_objects()` - ❌ NO Override Needed

**Why NOT Override?**

Both Doris and StarRocks use **MySQL-compatible information_schema**.

Base class implementation:
```python
def _get_data_objects(self, schema_name, ...):
    query = f"""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = '{schema_name}'
    """
    return self.fetchall(query)
```

This works for:
- MySQL
- MariaDB
- Doris
- ✅ StarRocks (same structure)

**Decision**: Don't override, use base class.

---

### Method #4: `delete_from()` - ⚠️ Optional Override

**The Complexity**:

StarRocks has **different DELETE capabilities** for different table types:

| Table Type | DELETE Subquery Support |
|------------|------------------------|
| Primary Key | ✅ YES |
| Unique Key | ❌ NO |
| Duplicate Key | ❌ NO |
| Aggregate | ❌ NO |

**Example**:
```sql
-- Primary Key table: Works!
DELETE FROM pk_table
WHERE id IN (SELECT id FROM blacklist);

-- Duplicate Key table: ERROR!
DELETE FROM dup_table
WHERE id IN (SELECT id FROM blacklist);
-- Must use:
DELETE FROM dup_table USING blacklist
WHERE dup_table.id = blacklist.id;
```

**Problem**: We don't know the table type at runtime!

**Solution Options**:

1. **Conservative (Recommended for MVP)**:
   - Use base class `delete_from()` - only simple conditions
   - OR: Always use USING syntax for subqueries (like Doris)

2. **Smart (Future Enhancement)**:
   - Query table type first
   - Use direct subquery for Primary Key tables
   - Use USING syntax for other tables

**Decision for MVP**: Don't override. Use base class.

---

### Method #5: `insert_overwrite_by_time_partition()` - ❌ NO Override Needed

**Why NOT?**

We configured this via class attribute:
```python
INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
```

Base class automatically does:
```python
if self.INSERT_OVERWRITE_STRATEGY == DELETE_INSERT:
    self.delete_from(table, where=condition)  # Step 1: Delete old data
    self.insert_append(table, query)          # Step 2: Insert new data
```

No override needed! This is **strategy pattern** in action.

---

## 🎯 Part 3: Expression Partitioning (Your Question #3)

### StarRocks Expression Partitioning

You mentioned StarRocks supports:
1. `date_trunc()` and other time functions
2. Multiple columns in partition expressions
3. Mixed column + function partitioning

**Examples**:
```sql
-- Expression partitioning (StarRocks 3.1+)
CREATE TABLE orders (
    order_id BIGINT,
    order_time DATETIME,
    amount DECIMAL
)
PARTITION BY RANGE(date_trunc('day', order_time)) (
    PARTITION p20240101 VALUES [("2024-01-01"), ("2024-01-02")),
    PARTITION p20240102 VALUES [("2024-01-02"), ("2024-01-03"))
);

-- Multi-column partitioning
PARTITION BY RANGE(dt, region) (
    ...
);

-- Mixed expression
PARTITION BY RANGE(year(dt), month(dt)) (
    ...
);
```

### How to Support This?

**Option 1: SQLMesh Model Level** (Recommended)

Users specify partition expressions in the model:
```sql
MODEL (
  name my_model,
  partitioned_by [FUNC('date_trunc', 'day', COLUMN('order_time'))],
  ...
);
```

SQLMesh passes this expression to the adapter, we just need to preserve it.

**Option 2: Adapter Level** (If needed)

Override `_build_partitioned_by_exp()` to handle expressions:
```python
def _build_partitioned_by_exp(self, partitioned_by, ...):
    # Check if partitioned_by contains function calls
    for expr in partitioned_by:
        if isinstance(expr, exp.Anonymous):  # Function call
            # Preserve the function expression
            ...
```

**Current Implementation**:

Let's check what Doris does - if it already handles this, we might not need to override.

**Decision**: Start without override, test if base class handles it. Add override only if needed.

---

## 🔧 Part 4: SQLGlot Dialect Enhancement

### Current SQLGlot Status

First, let's check if SQLGlot already has StarRocks support:

```python
# Check in your workspace
from sqlglot import dialects

print("Available dialects:", list(dialects.Dialects))
print("StarRocks supported:", "starrocks" in dialects.Dialects)
```

### If StarRocks Dialect Doesn't Exist

#### Step 1: Create StarRocks Dialect (Inherit from Doris)

**File**: `sqlglot/dialects/starrocks.py`

```python
"""
StarRocks SQL Dialect.

StarRocks is a fork of Apache Doris with enhancements:
- Native PRIMARY KEY support
- Enhanced analytical capabilities
- Better performance optimizations

Syntax is 90% compatible with Doris, with key differences:
1. PRIMARY KEY vs UNIQUE KEY
2. Enhanced materialized view syntax
3. Additional functions and optimizations
"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.doris import Doris
from sqlglot.tokens import TokenType


class StarRocks(Doris):
    """
    StarRocks SQL dialect.

    Most syntax is inherited from Doris.
    We only override the parts that differ.
    """

    class Parser(Doris.Parser):
        """
        Parser for StarRocks SQL.

        Differences from Doris:
        - Support PRIMARY KEY in table properties
        """

        # Add PRIMARY KEY to table properties
        TABLE_PROPERTIES = {
            **Doris.Parser.TABLE_PROPERTIES,
            "PRIMARY": lambda self: self._parse_primary_key(),
        }

        def _parse_primary_key(self) -> exp.PrimaryKeyColumnConstraint:
            """
            Parse: PRIMARY KEY (col1, col2, ...)
            """
            self._match(TokenType.KEY)
            return self.expression(
                exp.PrimaryKeyColumnConstraint,
                expressions=self._parse_wrapped_id_vars(),
            )

    class Generator(Doris.Generator):
        """
        SQL Generator for StarRocks.

        Differences from Doris:
        - Generate PRIMARY KEY instead of UNIQUE KEY
        """

        # Override: Generate PRIMARY KEY
        def primarykeycolumnconstraint_sql(
            self, expression: exp.PrimaryKeyColumnConstraint
        ) -> str:
            """
            Generate: PRIMARY KEY(col1, col2)

            Note: Doris would generate UNIQUE KEY instead.
            """
            columns = self.expressions(expression, flat=True)
            return f"PRIMARY KEY({columns})"

        # Optional: Override UNIQUE KEY generation if needed
        def uniquekeycolumnconstraint_sql(
            self, expression: exp.UniqueKeyColumnConstraint
        ) -> str:
            """
            StarRocks supports both UNIQUE KEY (legacy) and PRIMARY KEY.

            For UNIQUE KEY tables (legacy Unique Key model), generate:
            UNIQUE KEY(col1, col2)
            """
            columns = self.expressions(expression, flat=True)
            return f"UNIQUE KEY({columns})"

    # Inherit all function mappings from Doris
    # Add StarRocks-specific functions if needed
    class Tokenizer(Doris.Tokenizer):
        # StarRocks has same keywords as Doris
        KEYWORDS = {
            **Doris.Tokenizer.KEYWORDS,
            # Add StarRocks-specific keywords here if any
        }
```

#### Step 2: Register the Dialect

**File**: `sqlglot/dialects/__init__.py`

```python
from sqlglot.dialects.starrocks import StarRocks

# Add to dialects mapping
DIALECTS = {
    # ... existing dialects ...
    "starrocks": StarRocks,
    # ...
}
```

#### Step 3: Test the Dialect

**File**: `tests/dialects/test_starrocks.py`

```python
import unittest
from sqlglot import parse_one, transpile
from sqlglot.dialects import StarRocks


class TestStarRocks(unittest.TestCase):
    def test_primary_key_generation(self):
        """
        Test that PRIMARY KEY is generated correctly.
        """
        sql = "CREATE TABLE users (id INT, name VARCHAR(100)) PRIMARY KEY(id)"

        # Parse and regenerate
        parsed = parse_one(sql, dialect="starrocks")
        generated = parsed.sql(dialect="starrocks")

        self.assertIn("PRIMARY KEY", generated)
        self.assertNotIn("UNIQUE KEY", generated)

    def test_unique_key_legacy_support(self):
        """
        Test that UNIQUE KEY is still supported (for legacy Unique Key tables).
        """
        sql = "CREATE TABLE users (id INT, name VARCHAR(100)) UNIQUE KEY(id)"

        parsed = parse_one(sql, dialect="starrocks")
        generated = parsed.sql(dialect="starrocks")

        self.assertIn("UNIQUE KEY", generated)

    def test_distributed_by(self):
        """
        Test DISTRIBUTED BY clause (inherited from Doris).
        """
        sql = """
        CREATE TABLE users (
            id INT,
            name VARCHAR(100)
        )
        PRIMARY KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        """

        parsed = parse_one(sql, dialect="starrocks")
        generated = parsed.sql(dialect="starrocks")

        self.assertIn("DISTRIBUTED BY HASH", generated)
        self.assertIn("BUCKETS 10", generated)

    def test_partition_by_range(self):
        """
        Test PARTITION BY RANGE (inherited from Doris).
        """
        sql = """
        CREATE TABLE orders (
            order_id BIGINT,
            dt DATE
        )
        PRIMARY KEY(order_id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p20240101 VALUES [("2024-01-01"), ("2024-01-02"))
        )
        """

        parsed = parse_one(sql, dialect="starrocks")
        generated = parsed.sql(dialect="starrocks")

        self.assertIn("PARTITION BY RANGE", generated)

    def test_expression_partitioning(self):
        """
        Test expression-based partitioning (StarRocks 3.1+).
        """
        sql = """
        CREATE TABLE orders (
            order_id BIGINT,
            order_time DATETIME
        )
        PARTITION BY RANGE(date_trunc('day', order_time)) ()
        """

        parsed = parse_one(sql, dialect="starrocks")
        generated = parsed.sql(dialect="starrocks")

        self.assertIn("date_trunc", generated.lower())

    def test_transpile_from_standard_sql(self):
        """
        Test transpiling from standard SQL to StarRocks.
        """
        # Standard SQL with PRIMARY KEY
        standard_sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100)
        )
        """

        # Transpile to StarRocks
        starrocks_sql = transpile(standard_sql, read="", write="starrocks")[0]

        # StarRocks should have PRIMARY KEY in table properties
        self.assertIn("PRIMARY KEY", starrocks_sql)


if __name__ == "__main__":
    unittest.main()
```

### Minimal SQLGlot Changes Summary

**What needs to change**:

1. ✅ **Create StarRocks dialect file** (inherits 90% from Doris)
2. ✅ **Override PRIMARY KEY generation** (main difference)
3. ✅ **Add tests** (verify PRIMARY KEY vs UNIQUE KEY)
4. ✅ **Register dialect** (add to dialects mapping)

**What stays the same**:

- ❌ DISTRIBUTED BY syntax (same as Doris)
- ❌ PARTITION BY syntax (same as Doris)
- ❌ Data types (same as Doris)
- ❌ Functions (mostly same as Doris)
- ❌ Comment syntax (same as Doris)

### Expression Partitioning Support

For expression-based partitioning like:
```sql
PARTITION BY RANGE(date_trunc('day', order_time))
```

This should already work because:
1. SQLGlot can parse function calls
2. `date_trunc` is a standard function
3. PARTITION BY accepts expressions, not just column names

**Test it**:
```python
from sqlglot import parse_one

sql = "PARTITION BY RANGE(date_trunc('day', order_time))"
parsed = parse_one(sql, dialect="starrocks")
print(parsed)  # Should preserve the function call
```

If it doesn't work, we might need to extend the partition expression parser.

---

## 📋 Part 5: Implementation Checklist

### Phase 1: Minimal Viable Implementation (MVP)

- [x] Create `starrocks.py` adapter file
- [x] Implement `_create_table_from_columns()` (PRIMARY KEY support)
- [ ] Add `create_schema()` override (DATABASE keyword)
- [ ] Add `drop_schema()` override (DATABASE keyword)
- [ ] Test with StarRocks 3.5.3

### Phase 2: SQLGlot Integration

- [ ] Check if SQLGlot has StarRocks dialect
- [ ] If not: Create StarRocks dialect (inherit from Doris)
- [ ] Override PRIMARY KEY generation
- [ ] Add tests for PRIMARY KEY vs UNIQUE KEY
- [ ] Test expression partitioning
- [ ] Submit PR to SQLGlot

### Phase 3: Enhanced Features

- [ ] Add `delete_from()` with subquery support
- [ ] Add materialized view support
- [ ] Test partition expressions (date_trunc, etc.)
- [ ] Add multi-column partitioning tests
- [ ] Performance optimization

### Phase 4: Documentation & Testing

- [ ] Write comprehensive tests
- [ ] Document usage examples
- [ ] Create migration guide from Doris
- [ ] Add to SQLMesh documentation

---

## 🎯 Summary: What You Need to Implement

### In SQLMesh (starrocks.py):

**Mandatory**:
1. ✅ `_create_table_from_columns()` - Already done! (just calls base class)
2. ⚠️ `create_schema()` - Need to add (use DATABASE keyword)
3. ⚠️ `drop_schema()` - Need to add (use DATABASE keyword)

**Optional** (can add later):
4. `delete_from()` - For subquery support
5. `_build_table_properties_exp()` - If table properties differ from base class
6. `_create_materialized_view()` - If MV syntax differs

### In SQLGlot (if needed):

**Mandatory**:
1. Create `dialects/starrocks.py` (inherit from Doris)
2. Override `primarykeycolumnconstraint_sql()` method
3. Add tests

**Optional**:
4. Add StarRocks-specific functions
5. Enhance expression partitioning parser (if base parser doesn't handle it)

### Testing:

1. Test PRIMARY KEY table creation
2. Test UNIQUE KEY table creation (legacy)
3. Test DUPLICATE KEY table creation
4. Test PARTITION BY with expressions
5. Test DISTRIBUTED BY
6. Test DELETE operations

---

## 💡 Key Takeaways

1. **Minimal Override Strategy**: Only override what's different
   - PRIMARY KEY vs UNIQUE KEY → Override `_create_table_from_columns()`
   - DATABASE vs SCHEMA → Override `create_schema()`
   - Everything else → Use base class

2. **Strategy Pattern**: Use class attributes to configure behavior
   - `INSERT_OVERWRITE_STRATEGY` → No override needed
   - `SUPPORTS_TRANSACTIONS` → No override needed

3. **SQLGlot Separation**: Most SQL generation is in SQLGlot
   - Adapter: Orchestrates operations
   - SQLGlot: Generates SQL syntax
   - Clean separation of concerns

4. **Expression Partitioning**: Likely already supported
   - Test first before implementing
   - SQLGlot's parser is powerful

5. **Incremental Implementation**: Start simple, add features later
   - MVP: Just PRIMARY KEY support
   - Phase 2: SQLGlot dialect
   - Phase 3: Advanced features

| Feature | Doris | StarRocks | Impact |
|---------|-------|-----------|--------|
| **Primary Key** | Uses `UNIQUE KEY` | Native `PRIMARY KEY` support | ✅ Major difference - requires different handling |
| **DELETE Subquery** | Not supported | Supported for Primary Key tables only | ⚠️ Conditional handling needed |
| **Table Types** | Unique Key, Duplicate, Aggregate | Primary Key (enhanced), Unique, Duplicate, Aggregate | ℹ️ Primary Key replaces Unique Key |
| **Materialized Views** | ASYNC/SYNC | ASYNC/SYNC (similar syntax) | ✅ Likely same implementation |
| **DATABASE keyword** | DATABASE | DATABASE | ✅ Same |
| **information_schema** | MySQL-compatible | MySQL-compatible | ✅ Same |
| **DISTRIBUTED BY** | HASH/RANDOM | HASH/RANDOM | ✅ Same |
| **PARTITION BY** | RANGE/LIST | RANGE/LIST | ✅ Same |

## Implementation Strategy

**NOT Inheriting from Doris** - We are creating a standalone adapter that **references** Doris implementation but doesn't inherit.

Reasons:

1. PRIMARY KEY vs UNIQUE KEY is a fundamental difference
2. Cleaner code without workarounds for inheritance
3. Easier to maintain separate evolution paths
4. More explicit about what we support

## File Structure

```plain text
sqlmesh/core/engine_adapter/
├── starrocks.py          # Main adapter implementation
└── doris.py             # Reference implementation

tests/core/engine_adapter/
└── test_starrocks.py    # Test suite (TODO)
```

## Implemented Methods

### ✅ Fully Implemented

#### 1. `create_schema()`

- Uses `DATABASE` keyword instead of `SCHEMA`
- Same as Doris
- **Status**: Complete

#### 2. `drop_schema()`

- Uses `DATABASE` keyword
- No CASCADE support
- Same as Doris
- **Status**: Complete

#### 3. `_get_data_objects()`

- Uses `information_schema.tables`
- MySQL-compatible
- Same as Doris
- **Status**: Complete

#### 4. `_create_table_from_columns()` ⭐ **KEY DIFFERENCE**

- **StarRocks**: Passes `primary_key` directly to base class
- **Doris**: Converts `primary_key` to `unique_key` in table_properties
- **Status**: Complete

```python
# StarRocks implementation:
super()._create_table_from_columns(
    primary_key=primary_key,  # Pass as-is
    ...
)

# Doris implementation:
table_properties["unique_key"] = exp.Tuple(...)  # Convert
super()._create_table_from_columns(
    primary_key=None,  # Block base class
    ...
)
```

#### 5. `_build_table_propertiesd_exp()` ⭐ **KEY DIFFERENCE**

- **StarRocks**: Handles PRIMARY KEY natively (via base class)
- **StarRocks**: Handles UNIQUE KEY from table_properties (legacy Unique Key tables)
- **Doris**: Only handles UNIQUE KEY (converted from primary_key)
- **Status**: Complete (simplified version)

Handles:

- PRIMARY KEY (delegated to base class)
- UNIQUE KEY (for legacy tables)
- DUPLICATE KEY
- PARTITION BY RANGE/LIST
- DISTRIBUTED BY HASH
- COMMENT
- Materialized view properties (BUILD, REFRESH)

#### 6. `_parse_partition_expressions()`

- Parses RANGE(col) and LIST(col) syntax
- Extracts partition kind and column list
- Same as Doris
- **Status**: Complete

#### 7. `_build_partitioned_by_exp()`

- Builds PARTITION BY RANGE or LIST expression
- Same as Doris
- **Status**: Complete

#### 8. Comment Methods

- `_create_table_comment()`: ALTER TABLE MODIFY COMMENT
- `_build_create_comment_column_exp()`: ALTER TABLE MODIFY COLUMN ... COMMENT
- Same as Doris
- **Status**: Complete

#### 9. `create_table_like()`

- Uses CREATE TABLE ... LIKE syntax
- Same as Doris
- **Status**: Complete

### 🚧 Partially Implemented

#### 10. `delete_from()` ⚠️ **CONDITIONAL DIFFERENCE**

- **StarRocks Primary Key tables**: Support subquery
- **StarRocks Other tables**: Do NOT support subquery
- **Strategy**: Use USING syntax for all subqueries (conservative, same as Doris)
- **Status**: Framework complete, helper methods TODO

```python
# Implementation logic:
if no_condition:
    TRUNCATE TABLE
elif has_subquery:
    # Use USING syntax (safe for all table types)
    DELETE FROM t1 USING t2 WHERE t1.id = t2.id
else:
    # Standard DELETE (works for all table types)
    DELETE FROM t WHERE condition
```

#### 11. `create_view()` / `_create_materialized_view()`

- StarRocks supports ASYNC and SYNC materialized views
- Syntax similar to Doris but needs verification
- **Status**: Framework complete, implementation TODO

### ❌ Not Yet Implemented

#### 12. `drop_view()`

- Drop materialized views
- **Status**: TODO

#### 13. Helper methods for DELETE

- `_find_subquery_in_condition()`
- `_execute_delete_with_subquery()`
- **Status**: TODO (copy from Doris)

## SQLGlot Dialect Support

### Current Status

Check if SQLGlot already has StarRocks dialect support:

```python
from sqlglot import dialects
print("starrocks" in dialects.Dialects)
```

### If Not Supported

We can temporarily use Doris dialect with custom handling:

```python
# In starrocks.py
DIALECT = "doris"  # Temporarily use Doris dialect

# Generate SQL with explicit dialect
exp.Create(...).sql(dialect="doris", identify=True)
```

### Future Enhancement

Create a dedicated StarRocks dialect in SQLGlot:

```python
# In SQLGlot repository
from sqlglot.dialects.doris import Doris

class StarRocks(Doris):
    """StarRocks SQL dialect."""

    class Generator(Doris.Generator):
        # Override PRIMARY KEY generation
        def primarykey_sql(self, expression: exp.PrimaryKey) -> str:
            return f"PRIMARY KEY({self.expressions(expression)})"
```

## Testing Strategy

### Test Cases to Implement

1. **Table Creation**

   ```python
   def test_create_table_with_primary_key():
       # Should generate: CREATE TABLE ... PRIMARY KEY(id)

   def test_create_table_with_unique_key():
       # Should generate: CREATE TABLE ... UNIQUE KEY(id)

   def test_create_table_with_duplicate_key():
       # Should generate: CREATE TABLE ... DUPLICATE KEY(id)
   ```

2. **Partitioning**

   ```python
   def test_create_table_with_range_partition():
       # PARTITION BY RANGE(dt) (...)

   def test_create_table_with_list_partition():
       # PARTITION BY LIST(region) (...)
   ```

3. **Distribution**

   ```python
   def test_create_table_with_hash_distribution():
       # DISTRIBUTED BY HASH(id) BUCKETS 10
   ```

4. **DELETE Operations**

   ```python
   def test_delete_simple_condition():
       # DELETE FROM t WHERE id = 1

   def test_delete_with_subquery():
       # DELETE FROM t USING (...) WHERE ...

   def test_delete_truncate():
       # TRUNCATE TABLE t
   ```

5. **Materialized Views**

   ```python
   def test_create_async_materialized_view():
       # CREATE MATERIALIZED VIEW ... REFRESH ASYNC

   def test_create_sync_materialized_view():
       # CREATE MATERIALIZED VIEW ... (rollup)
   ```

### Test Environment

- StarRocks 3.3+ (you have 3.5.3 available)
- SQLMesh test framework
- Compare against Doris adapter tests

## Next Steps

### Phase 1: Complete Core Methods (Current)

- [x] `_create_table_from_columns()` - PRIMARY KEY support
- [x] `_build_table_properties_exp()` - Table properties
- [x] `_parse_partition_expressions()` - Partition parsing
- [x] `_build_partitioned_by_exp()` - Partition building
- [ ] `delete_from()` - Complete helper methods
- [ ] `_create_materialized_view()` - Verify syntax differences

### Phase 2: Testing

- [ ] Write unit tests for table creation
- [ ] Write unit tests for DELETE operations
- [ ] Write integration tests with StarRocks 3.5.3
- [ ] Compare behavior with Doris adapter

### Phase 3: Documentation

- [ ] Add docstrings with examples
- [ ] Update SQLMesh documentation
- [ ] Create migration guide from Doris

### Phase 4: SQLGlot Integration

- [ ] Check current SQLGlot StarRocks support
- [ ] Submit PR to SQLGlot if needed
- [ ] Update adapter to use dedicated dialect

### Phase 5: Advanced Features

- [ ] Materialized view refresh strategies
- [ ] Optimize for Primary Key table features
- [ ] Support StarRocks-specific functions
- [ ] Warehouse support (StarRocks 3.3+)

## Reference Implementation Comparison

### Creating a Primary Key Table

**StarRocks Adapter (New)**:

```python
adapter.create_table(
    table_name="my_table",
    columns_to_types={"id": "INT", "name": "VARCHAR(100)"},
    primary_key=("id",)
)
# Generates: CREATE TABLE my_table (id INT, name VARCHAR(100)) PRIMARY KEY(id)
```

**Doris Adapter (Reference)**:

```python
adapter.create_table(
    table_name="my_table",
    columns_to_types={"id": "INT", "name": "VARCHAR(100)"},
    primary_key=("id",)
)
# Generates: CREATE TABLE my_table (id INT, name VARCHAR(100)) UNIQUE KEY(id)
```

### Key Code Differences

#### StarRocks `_create_table_from_columns()`

```python
def _create_table_from_columns(self, ..., primary_key=None, **kwargs):
    # Pass primary_key directly - base class handles PRIMARY KEY
    super()._create_table_from_columns(
        primary_key=primary_key,  # ✅ As-is
        ...
    )
```

#### Doris `_create_table_from_columns()`

```python
def _create_table_from_columns(self, ..., primary_key=None, **kwargs):
    table_properties = kwargs.get("table_properties", {})

    # Convert primary_key to unique_key
    if primary_key:
        table_properties["unique_key"] = exp.Tuple(...)  # 🔄 Convert

    kwargs["table_properties"] = table_properties

    super()._create_table_from_columns(
        primary_key=None,  # ❌ Block base class
        ...
    )
```

## Configuration Examples

### config.yaml

```yaml
gateways:
  starrocks_gateway:
    connection:
      type: starrocks
      host: localhost
      port: 9030
      user: root
      password: ''
      database: default_catalog.my_db
```

### Model Definition

```sql
MODEL (
  name my_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column dt
  ),
  primary_key (id),  -- Will use PRIMARY KEY in StarRocks
  partitioned_by (dt),
  distributed_by (kind='HASH', expressions=['id'], buckets=10)
);

SELECT
  id,
  name,
  amount,
  dt
FROM source_table
WHERE dt BETWEEN @start_date AND @end_date
```

## Notes

1. **PRIMARY KEY Constraint**: StarRocks PRIMARY KEY tables require partition columns to be in the primary key
2. **DELETE Performance**: Primary Key tables support efficient DELETE by primary key
3. **Materialized Views**: StarRocks 3.3+ has enhanced MV capabilities (text-based rewrite, view-based MV)
4. **Warehouse Feature**: StarRocks 3.3+ introduces warehouse concept for resource isolation

## Contributors

- Implementation: Based on Doris adapter by SQLMesh team
- StarRocks Specifics: [Your Name]
- Testing: TBD

## References

- [StarRocks Documentation](https://docs.starrocks.io/)
- [StarRocks vs Doris Comparison](https://forum.starrocks.io/t/faq-apache-doris-vs-starrocks/128)
- [StarRocks 3.3 Release Notes](https://docs.starrocks.io/releasenotes/release-3.3/)
- [SQLMesh Engine Adapter Architecture](https://sqlmesh.readthedocs.io/)
