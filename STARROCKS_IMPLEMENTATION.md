# StarRocks Engine Adapter Implementation Guide

> **Status**: Implementation Complete (Core Features)
> **Last Updated**: 2024-11-26
> **Target**: StarRocks 3.3+, SQLMesh 0.x
> **Key Insight**: StarRocks is more SQL-standard compliant than Doris - fewer overrides needed!

## 🔑 Key Differences from Doris Adapter

| Aspect | Doris | StarRocks | Impact |
|--------|-------|-----------|--------|
| **create_schema()** | Override needed (DATABASE only) | ❌ No override (supports both SCHEMA/DATABASE) | Simpler |
| **drop_schema()** | Override needed (DATABASE only) | ❌ No override (supports both SCHEMA/DATABASE) | Simpler |
| **PRIMARY KEY** | Converts to UNIQUE KEY | ✅ Native support, pass through | Critical |
| **Column ordering** | Not required | ✅ Required (keys first) | Must handle |
| **DELETE WHERE** | Basic support | ✅ Enhanced (needs WHERE cleaning) | More complex |
| **Total overrides** | 7 methods | 5 methods (+ 3 helpers) | Cleaner code |

## 🚧 Limitations

### AGGREGATE KEY Not Supported

AGGREGATE KEY tables require specifying aggregation functions (SUM/MAX/MIN/REPLACE) at the column level, which is not supported in SQLMesh's model syntax. Use PRIMARY KEY or DUPLICATE KEY instead. If attempted, SQLMesh will raise a clear error with alternatives.

## ✅ Implementation Status

### Core Features - **COMPLETE**

| Component | Status | Location | Description |
|-----------|--------|----------|-------------|
| **Declarative Type System** | ✅ Complete | Lines 33-1095 | Type validators with validate/normalize pattern |
| **Property Specifications** | ✅ Complete | Lines 1101-1306 | Input/Output specs for all properties |
| **Property Validation** | ✅ Complete | Lines 1311-1570 | Centralized validation helpers |
| **Table Creation** | ✅ Complete | Lines 2018-2134 | Column reordering + unified parameter handling |
| **Table Properties** | ✅ Complete | Lines 2136-2777 | All properties orchestration |
| **DELETE Operations** | ✅ Complete | Lines 1814-1964 | WHERE clause cleaning + TRUNCATE |
| **Index Operations** | ✅ Complete | Lines 1782-1812 | No-op with logging |
| **Schema Operations** | ✅ Base class | N/A | Uses base class (SR supports CREATE SCHEMA) |

### Supported Properties - **COMPLETE**

| Property | Input Types | Status | Notes |
|----------|------------|--------|-------|
| `primary_key` | Column list, string | ✅ | POST_SCHEMA location |
| `duplicate_key` | Column list, string | ✅ | For DUPLICATE KEY tables |
| `unique_key` | Column list, string | ✅ | For legacy UNIQUE KEY tables |
| `partitioned_by` | Columns, RANGE(), LIST(), expressions | ✅ | All 3 partition types |
| `partitions` | List of partition definitions | ✅ | Pre-created partitions |
| `distributed_by` | Structured tuple, HASH(), RANDOM, strings | ✅ | Complex parsing with BUCKETS |
| `clustered_by` / `order_by` | Column list | ✅ | Alias support |
| Generic properties | String, literal, identifier | ✅ | Auto-handled via SPEC |

## 📋 Quick Reference: Hierarchical Function Call Map

```
┌─────────────────────────────────────────────────────────────────────┐
│  Legend:                                                            │
│  ✅ = Override in StarRocksEngineAdapter (Our Implementation)      │
│  🔧 = Helper Method (Called by Override Methods)                   │
│  📞 = Called via super() (Base Class Method)                       │
│  ❌ = No Override Needed (Use Base Class Directly)                 │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 1: Public API Methods (Entry Points)                         │
└─────────────────────────────────────────────────────────────────────┘

❌ create_schema()                    [Base class - SR supports CREATE SCHEMA]
   └📞 _create_schema()              [base.py: L1388-1411]
       └─ execute(CREATE SCHEMA ...)

❌ drop_schema()                      [Base class - SR supports DROP SCHEMA]
   └📞 _drop_object()                [base.py: L1132-1156]
       └─ execute(DROP SCHEMA ...)

✅ create_table()                     [Override path via _create_table_from_columns]
   ├❌ create_table()                [base.py: L684-733 - Router method]
   │   ├─ if is_ctas:
   │   │   └📞 _create_table_from_source()   [base.py: L829-903]
   │   └─ else:
   │       └✅ _create_table_from_columns()  [⭐ OVERRIDE - starrocks.py: L584-681]
   │           │
   │           ├🔧 _extract_and_validate_key_columns()  [starrocks.py: L683-755]
   │           │   └🔧 _expr_to_column_tuple()         [starrocks.py: L757-797]
   │           │
   │           ├🔧 _reorder_columns_for_key()          [starrocks.py: L799-858]
   │           │
   │           └📞 super()._create_table_from_columns() [base.py: L736-804]
   │               ├📞 _build_schema_exp()             [base.py: L806-827]
   │               ├📞 _create_table()                 [base.py: L961-997]
   │               │   └📞 _build_create_table_exp()   [base.py: L999-1037]
   │               │       ├✅ _build_table_properties_exp()  [⭐ OVERRIDE - starrocks.py: L477-582]
   │               │       │   │                               [📞 Called by base._build_create_table_exp L1020]
   │               │       │   ├─ Handle DISTRIBUTED BY
   │               │       │   ├─ Handle DUPLICATE/UNIQUE KEY
   │               │       │   ├─ Convert literal properties
   │               │       │   └📞 _properties_to_expressions() [base.py: L2786-2830]
   │               │       │
   │               │       └─ Build exp.Create(...)
   │               │
   │               └─ Post-table creation (COMMENT handling):
   │                   ├─ if COMMENT_CREATION_TABLE.is_comment_command_only:
   │                   │   ├📞 _create_table_comment()      [base.py: L798]
   │                   │   │   └🔧 _build_create_comment_table_exp()  [⚠️ Override recommended]
   │                   │   └📞 _create_column_comments()    [base.py: L804]
   │                   │       └🔧 _build_create_comment_column_exp() [⚠️ Override recommended]
   │                   │
   │                   └─ Note: StarRocks uses IN_SCHEMA_DEF_CTAS, so these are NOT called
   │                            Comments are included directly in CREATE TABLE via SchemaCommentProperty

✅ delete_from()                      [⭐ OVERRIDE - starrocks.py: L218-236]
   └─ if WHERE TRUE:
       └─ execute(TRUNCATE TABLE)
      else:
       └📞 super().delete_from()      [base.py: L2042-2095]

✅ execute()                          [⭐ OVERRIDE - starrocks.py: L238-280]
   └─ Strip FOR UPDATE locks
       └📞 super().execute()          [base.py: L553-612]

✅ create_index()                     [⭐ OVERRIDE - starrocks.py: L191-216]
   └─ Log warning and return (no-op)

❌ insert_append()                    [Base class works]
   └📞 insert_append()               [base.py: L1676-1687]
       └─ execute(INSERT INTO SELECT)

❌ insert_overwrite_by_time_partition() [Base class works - uses strategy]
   └📞 insert_overwrite_by_time_partition() [base.py: L2193-2289]
       ├─ if INSERT_OVERWRITE_STRATEGY == DELETE_INSERT:
       │   ├✅ delete_from()           [Our override handles it]
       │   └📞 insert_append()         [Base class]
       └─ else if native INSERT OVERWRITE:
           └─ execute(INSERT OVERWRITE)

❌ _get_data_objects()                [Base class works]
   └📞 _get_data_objects()           [base.py: L1489-1515]
       └─ Query information_schema.tables

⚠️ create_view()                      [Base class likely works]
   └📞 create_view()                 [base.py: L1087-1166]
       ├📞 _create_view()            [base.py: L1168-1203]
       └⚠️ _create_materialized_view() [TODO - For MV REFRESH]

┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 2: Core Override Methods (Implementation Details)            │
└─────────────────────────────────────────────────────────────────────┘

✅ _create_table_from_columns()       [⭐ OVERRIDE - starrocks.py: L584-681]
   Purpose: Create table with column definitions
   Override Reason:
     1. Column reordering (key columns must be first)
     2. PRIMARY KEY support (pass to base, don't convert)

✅ _build_table_properties_exp()      [⭐ OVERRIDE - starrocks.py: L477-582]
   Purpose: Build PROPERTIES clause for CREATE TABLE
   Override Reason:
     1. Handle DISTRIBUTED BY nested tuple
     2. Handle literal properties (replication_num, etc)
     3. Handle DUPLICATE KEY / UNIQUE KEY / PRIMARY KEY

❌ _build_create_comment_table_exp() [base.py: L2962-2969 → ⚠️ Override recommended]
   Purpose: Build ALTER TABLE COMMENT SQL for table comment modification
   Override Reason: StarRocks uses non-standard syntax (ALTER TABLE {table} COMMENT = '{comment}')
   Note: Typically NOT called (StarRocks uses IN_SCHEMA_DEF_CTAS)

❌ _build_create_comment_column_exp() [base.py: L2984-2991 → ⚠️ Override recommended]
   Purpose: Build ALTER TABLE MODIFY COLUMN SQL for column comment modification
   Override Reason: StarRocks requires column type in MODIFY COLUMN statement
   Note: Typically NOT called (StarRocks uses IN_SCHEMA_DEF_CTAS)

✅ delete_from()                      [⭐ OVERRIDE - starrocks.py: L218-236]
   Purpose: Handle DELETE operations (WHERE TRUE → TRUNCATE)
   Override Reason: StarRocks doesn't support WHERE TRUE

   Called by:
     📞 insert_overwrite_by_time_partition() [base.py: L2193-2289]
     📞 User code / SQLMesh internals

✅ execute()                          [⭐ OVERRIDE - starrocks.py: L238-280]
   Purpose: Strip FOR UPDATE from queries
   Override Reason: StarRocks OLAP doesn't support row locks

   Logic:
     for expression in expressions:
         if isinstance(expression, exp.Select):
             if expression.args.get("locks"):
                 expression.set("locks", None)  # Remove FOR UPDATE
     📞 super().execute(processed_expressions)

✅ create_index()                     [⭐ OVERRIDE - starrocks.py: L191-216]
   Purpose: Prevent CREATE INDEX execution (no-op)
   Override Reason: StarRocks doesn't support standalone indexes

   Called by:
     📞 SQLMesh state table initialization
     📞 Model with explicit index definitions


┌─────────────────────────────────────────────────────────────────────┐
│ LEVEL 3: Helper Methods (Internal Utilities)                       │
└─────────────────────────────────────────────────────────────────────┘

🔧 _extract_and_validate_key_columns()  [starrocks.py: L683-755]
   Purpose: Extract key definition from table_properties

   Input: table_properties dict, primary_key tuple
   Output: (key_type, key_columns)

   Logic:
     1. Check for conflicts (can't have PK + UK + DK simultaneously)
     2. Priority: parameter primary_key > table_properties primary_key
     3. Extract from: primary_key, unique_key, duplicate_key
     4. Call 🔧 _expr_to_column_tuple() to parse
     5. Return ("primary_key" | "unique_key" | "duplicate_key", columns)

🔧 _expr_to_column_tuple()  [starrocks.py: L757-797]
   Purpose: Normalize key expressions to column name tuple

   Input: Expression (Tuple | list | Column | str)
   Output: Tuple[str, ...]  # Column names

   Handles:
     - exp.Tuple(expressions=[Column(...), ...])  → Extract names
     - [Column(...), ...]                         → Extract names
     - Column(...)                                → Single name
     - "col_name"                                 → Single name

🔧 _reorder_columns_for_key()  [starrocks.py: L799-858]
   Purpose: Reorder columns so key columns come first

   Input: columns dict, key_columns tuple, key_type str
   Output: Reordered columns dict

   StarRocks Constraint:
     ALL key types (PRIMARY/UNIQUE/DUPLICATE/AGGREGATE) require:
     - Key columns MUST be first N columns
     - Order MUST match KEY clause order

   Example:
     Input:  {"customer_id": INT, "order_id": BIGINT, "dt": DATE}
     Keys:   ("order_id", "dt")
     Output: {"order_id": BIGINT, "dt": DATE, "customer_id": INT}

┌─────────────────────────────────────────────────────────────────────┐
│ Methods NOT Needing Override (Use Base Class)                      │
└─────────────────────────────────────────────────────────────────────┘

❌ columns()                [base.py: L1517-1543]
   → Query: DESCRIBE TABLE or information_schema.columns

❌ table_exists()           [base.py: L1476-1487]
   → Query: information_schema.tables or SHOW TABLES

❌ fetchall() / fetchone()  [base.py: L497-551]
   → MySQL protocol compatible

❌ _build_partitioned_by_exp() [base.py: L2757-2784]
   → Should handle expression partitioning

❌ create_table_like()      [base.py: L1039-1054]
   → Uses CREATE TABLE ... LIKE syntax

❌ _create_table_comment()  [base.py: L2971-2982]
   → Uses _build_create_comment_table_exp instead

❌ _properties_to_expressions() [base.py: L2786-2830]
   → Converts dict properties to exp.Property list
```

---

## 🚨 Key Corrections from Review

### ✅ VERIFIED: create_schema() - NO Override Needed

**Initial assumption**: StarRocks needs `CREATE DATABASE` (like Doris)
**Reality**: StarRocks 3.x+ supports BOTH `CREATE SCHEMA` and `CREATE DATABASE`
**Action**: Use base class implementation - generates `CREATE SCHEMA` which works perfectly
**Status**: ✅ Verified working

### ✅ VERIFIED: drop_schema() - NO Override Needed

**Initial assumption**: StarRocks needs `DROP DATABASE` (like Doris)
**Reality**: StarRocks 3.x+ supports BOTH `DROP SCHEMA` and `DROP DATABASE`
**Action**: Use base class implementation - generates `DROP SCHEMA` which works perfectly
**Status**: ✅ Verified working

### ✅ Correct: _create_table_from_columns() Override

**Why needed**: Column reordering for key columns (StarRocks-specific constraint)
**Implementation**: ✅ Complete in starrocks.py

### ✅ Correct: _build_table_properties_exp() Override

**Why needed**: Handle DISTRIBUTED BY nested tuple, literal properties
**Implementation**: ✅ Complete in starrocks.py

### ✅ Correct: delete_from() Override

**Why needed**: WHERE TRUE → TRUNCATE TABLE conversion
**Implementation**: ✅ Complete in starrocks.py

### ✅ Correct: execute() Override

**Why needed**: Strip FOR UPDATE (OLAP doesn't support row locks)
**Implementation**: ✅ Complete in starrocks.py

### ✅ Correct: create_index() Override

**Why needed**: Prevent execution (SR doesn't support standalone indexes)
**Implementation**: ✅ Complete in starrocks.py

---

## 📊 Implementation Summary

**Total Overrides**: 5 core methods + 3 helper methods

### Core Override Methods

| Method | Status | Reason | Line Reference |
|--------|--------|--------|----------------|
| `_create_table_from_columns()` | ✅ Complete | Column reordering + PRIMARY KEY | starrocks.py: L584-681 |
| `_build_table_properties_exp()` | ✅ Complete | DISTRIBUTED BY, properties | starrocks.py: L477-582 |
| `delete_from()` | ✅ Complete | WHERE TRUE → TRUNCATE | starrocks.py: L218-236 |
| `execute()` | ✅ Complete | Strip FOR UPDATE | starrocks.py: L238-280 |
| `create_index()` | ✅ Complete | Skip execution (no-op) | starrocks.py: L191-216 |

### Helper Methods

| Method | Status | Purpose | Line Reference |
|--------|--------|---------|----------------|
| `_extract_and_validate_key_columns()` | ✅ Complete | Extract key definitions | starrocks.py: L683-755 |
| `_expr_to_column_tuple()` | ✅ Complete | Parse key expressions | starrocks.py: L757-797 |
| `_reorder_columns_for_key()` | ✅ Complete | Reorder columns | starrocks.py: L799-858 |

### Methods Using Base Class (No Override)

| Method | Verified | Reason |
|--------|----------|--------|
| `create_schema()` | ✅ Yes | SR supports CREATE SCHEMA |
| `drop_schema()` | ✅ Yes | SR supports DROP SCHEMA |
| `insert_append()` | ✅ Yes | Standard INSERT INTO SELECT |
| `insert_overwrite_by_time_partition()` | ✅ Yes | Uses DELETE_INSERT strategy |
| `_get_data_objects()` | ✅ Yes | MySQL-compatible information_schema |

---

## 📋 File Locations Quick Reference

```
sqlmesh/core/engine_adapter/
├── starrocks.py                    ✅ Main implementation
│   ├── L191-216:  create_index()               (✅ Override - no-op)
│   ├── L218-236:  delete_from()                (✅ Override - TRUNCATE)
│   ├── L238-280:  execute()                    (✅ Override - strip FOR UPDATE)
│   ├── L477-582:  _build_table_properties_exp() (✅ Override - properties)
│   ├── L584-681:  _create_table_from_columns() (✅ Override - CORE)
│   ├── L683-755:  _extract_and_validate_key_columns() (🔧 Helper)
│   ├── L757-797:  _expr_to_column_tuple()      (🔧 Helper)
│   └── L799-858:  _reorder_columns_for_key()   (🔧 Helper)
│
├── base.py                         ❌ Base class (no changes needed)
│   ├── L684-733:   create_table()             (Router - uses our overrides)
│   ├── L1388-1411: _create_schema()           (❌ Works as-is)
│   ├── L1132-1156: _drop_object()             (❌ Works as-is)
│   └── L2786-2830: _properties_to_expressions() (Used by our override)
│
└── doris.py                        📚 Reference (different approach)
    ├── create_schema()          (Doris: DATABASE; StarRocks: both work)
    ├── drop_schema()            (Doris: DATABASE; StarRocks: both work)
    └── _create_table_from_columns() (Doris: PK→UK conversion; StarRocks: direct)

mytest/test_model/
├── models/
│   ├── sr_primary_key.sql              Test PRIMARY KEY
│   ├── sr_duplicate_key.sql            Test DUPLICATE KEY
│   └── sr_distributed.sql              Test DISTRIBUTED BY
├── test_2_parse_model.py           Verify model parsing
├── test_4_direct_adapter.py        Full integration test
└── test_column_reordering.py       Column ordering tests

Root documentation:
├── STARROCKS_IMPLEMENTATION.md     📗 Implementation guide (this file)
├── IMPL_CALL_HIERARCHY_NEW.md      📘 Concise call hierarchy reference
└── starrocks_design.md             📘 Design decisions & rationale
```

---

## 📚 Part 1: Understanding Method Override Strategy

### Why Do We Need to Override Methods?

```
┌─────────────────────────────────────────────────────────────┐
│  Decision Tree: Should I Override This Method?             │
└─────────────────────────────────────────────────────────────┘

1. Does StarRocks use DIFFERENT SQL syntax than base class?
   ├─ YES → Override needed
   │  Examples:
   │  • _build_table_properties_exp(): DISTRIBUTED BY nested tuple
   │  • _create_table_from_columns(): Column reordering for keys
   │
   └─ NO → Check next question

2. Does StarRocks have DIFFERENT constraints/requirements?
   ├─ YES → Override needed
   │  Examples:
   │  • delete_from(): WHERE TRUE not supported → use TRUNCATE
   │  • execute(): FOR UPDATE not supported (OLAP database)
   │  • create_index(): Standalone indexes not supported
   │
   └─ NO → Check next question

3. Can base class behavior be configured via class attributes?
   ├─ YES → Just set the attribute, NO override needed
   │  Examples:
   │  • INSERT_OVERWRITE_STRATEGY = DELETE_INSERT → Base class handles it
   │  • SUPPORTS_TRANSACTIONS = False → Base handles transactions
   │
   └─ NO → Override needed

4. Is the method implementation database-agnostic?
   └─ YES → NO override needed, use base class
      Examples:
      • fetchall(): Just returns query results
      • create_schema(): StarRocks supports both SCHEMA and DATABASE
      • table_exists(): information_schema query works
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

## 🔍 Part 2: Detailed Method Analysis

### Schema Management

**✅ VERIFIED**: Both `create_schema()` and `drop_schema()` work with base class implementation.

StarRocks 3.x+ supports both standard SQL (SCHEMA) and MySQL-compatible (DATABASE) keywords as synonyms.

### Method #1: `create_schema()` / `drop_schema()` - ❌ NO Override Needed

**Base Class Implementation**:

```python
# sqlmesh/core/engine_adapter/base.py
def create_schema(self, schema_name, ...):
    return self._create_schema(
        schema_name=schema_name,
        kind="SCHEMA",  # ✅ StarRocks supports CREATE SCHEMA
        ...
    )

def drop_schema(self, schema_name, ...):
    return self._drop_object(
        name=schema_name,
        kind="SCHEMA",  # ✅ StarRocks supports DROP SCHEMA
        ...
    )
```

**Why NO Override?**

- **StarRocks 3.x+ supports BOTH syntaxes**:
  - `CREATE SCHEMA my_database` ✅ (Standard SQL - what base class generates)
  - `CREATE DATABASE my_database` ✅ (MySQL-compatible - also works)
- Base class generates `CREATE/DROP SCHEMA`, which works perfectly
- **Doris needed override** because older versions only supported `DATABASE` keyword
- **StarRocks is more SQL-standard compliant** - both forms are synonyms

**Verification**:

```sql
-- Both syntaxes work in StarRocks 3.x:
CREATE SCHEMA test_db;    -- ✅ Base class generates this
CREATE DATABASE test_db;  -- ✅ Also works (synonym)

DROP SCHEMA test_db;      -- ✅ Base class generates this
DROP DATABASE test_db;    -- ✅ Also works (synonym)
```

**Decision**: **Don't override**. Use base class implementation.

---

### Method #2: `_create_table_from_columns()` - ✅ MUST Override

**Why Override `_create_table_from_columns()`?**

StarRocks has a critical constraint: **Key columns MUST appear first** in the table definition.

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

**StarRocks Implementation**:

```python
# StarRocks: Column reordering + pass PRIMARY KEY to base class
def _create_table_from_columns(self, ..., primary_key=None, **kwargs):
    # 1. Extract key columns (primary_key, unique_key, or duplicate_key)
    key_type, key_columns = self._extract_and_validate_key_columns(
        table_properties, primary_key
    )

    # 2. Reorder columns: key columns MUST come first
    if key_columns:
        columns = self._reorder_columns_for_key(columns, key_columns, key_type)

    # 3. Pass to base class (handles PRIMARY KEY natively)
    super()._create_table_from_columns(
        columns=columns,
        primary_key=primary_key,  # ✅ Pass as-is, no conversion
        **kwargs
    )
    # Result: CREATE TABLE t (id BIGINT, name VARCHAR, ...) PRIMARY KEY(id)
```

**Key Differences from Doris**:

1. **No PRIMARY KEY → UNIQUE KEY conversion** (StarRocks supports PRIMARY KEY)
2. **Column reordering required** (StarRocks constraint)
3. **Simpler logic** (just reorder and delegate)

### DELETE Operations

**Why Override `delete_from()`?**

StarRocks has specific restrictions on DELETE WHERE clauses:

1. **WHERE TRUE not supported** → Use TRUNCATE TABLE instead
2. **Non-PRIMARY KEY tables**: BETWEEN not supported in DELETE WHERE
3. **Boolean literals not supported** in WHERE clauses

**Implementation**: Clean WHERE clause and delegate to base class or use TRUNCATE.

---

### Method #4: `insert_overwrite_by_time_partition()` - ❌ NO Override Needed

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

### Phase 1: Core Implementation ✅ **COMPLETE**

**Declarative Type System** (2000+ lines):

- [x] Base `DeclarativeType` with validate/normalize separation
- [x] Primitive types: String, Literal, Identifier, Column, Eq, Enum, Func
- [x] Combinators: AnyOf (union), SequenceOf (lists)
- [x] Structured types: StructuredTupleType, Field definitions
- [x] Distribution types: DistributionTupleInputType, DistributionTupleOutputType

**Property System**:

- [x] PropertySpecs with INPUT/OUTPUT specifications
- [x] PropertyValidator with centralized helpers
- [x] Alias support (partitioned_by/partition_by, clustered_by/order_by)
- [x] Invalid name checking and error guidance
- [x] Mutual exclusion validation

**Core Adapter Methods**:

- [x] `_create_table_from_columns()` - column reordering + unified parameter handling
- [x] `_build_table_properties_exp()` - orchestrates all properties
- [x] `_build_table_key_property()` - PRIMARY/DUPLICATE/UNIQUE keys
- [x] `_build_partition_property()` - RANGE/LIST/Expression partitions
- [x] `_build_distributed_by_property()` - complex distribution parsing
- [x] `_build_order_by_property()` - clustering with alias support
- [x] `_build_other_properties()` - generic properties via SPEC
- [x] `delete_from()` - WHERE clause cleaning + TRUNCATE
- [x] `execute()` - strip FOR UPDATE
- [x] `create_index()` - no-op with logging
- [x] Helper methods: `_extract_and_validate_key_columns()`, `_reorder_columns_for_key()`
- [x] **Verified**: `create_schema()` works with base class

## 🎯 Next Steps

### Testing (Phase 2)

- [ ] Write integration tests with StarRocks 3.5.3
- [ ] Test all table key types (PRIMARY, DUPLICATE, UNIQUE)
- [ ] Test all partition types (RANGE, LIST, Expression)
- [ ] Test distribution patterns (HASH with buckets, RANDOM)
- [ ] Test property alias handling
- [ ] Compare behavior with expected StarRocks SQL

### Documentation (Phase 3)

- [ ] Add comprehensive docstring examples
- [ ] Create user guide with SQLMesh model examples
- [ ] Document property input formats and validation

### SQLGlot Enhancement (Phase 4)

- [ ] Verify SQLGlot StarRocks dialect completeness
- [ ] Test PRIMARY KEY generation in POST_SCHEMA location
- [ ] Submit PR if improvements needed

## 🎯 Summary: Key Implementation Points

### What Makes StarRocks Different

1. **More SQL-Standard Compliant than Doris**
   - Supports both `CREATE SCHEMA` and `CREATE DATABASE` (Doris: DATABASE only)
   - Native `PRIMARY KEY` support (Doris: converts to UNIQUE KEY)
   - Result: Fewer adapter overrides needed

2. **Critical StarRocks Constraints**
   - **Column Ordering**: Key columns MUST be first in table definition
   - **DELETE WHERE**: No WHERE TRUE, no BETWEEN for non-PK tables, no boolean literals
   - **No Standalone Indexes**: Must use PRIMARY KEY or define in CREATE TABLE
   - **No FOR UPDATE**: OLAP database, no row-level locking

3. **Implementation Strategy**
   - **Minimal Overrides**: Only 5 core methods (vs Doris: 7+)
   - **Delegate to Base**: Use base class whenever possible
   - **Helper Methods**: 3 helpers for complex operations (column reordering, key extraction)

### Required Overrides

| Method | Purpose | Complexity |
|--------|---------|------------|
| `_create_table_from_columns()` | Column reordering for keys | Medium |
| `_build_table_properties_exp()` | Handle DISTRIBUTED BY, properties | High |
| `delete_from()` | WHERE clause cleaning, TRUNCATE | Low |
| `execute()` | Strip FOR UPDATE locks | Low |
| `create_index()` | No-op with logging | Trivial |

### Not Needed (Use Base Class)

- `create_schema()` / `drop_schema()` - StarRocks supports standard SQL
- `insert_append()` - Standard INSERT INTO SELECT
- `insert_overwrite_by_time_partition()` - Uses DELETE_INSERT strategy
- `_get_data_objects()` - MySQL-compatible information_schema
- `table_exists()`, `columns()`, `fetchall()` - All work as-is

## 📖 Usage Examples

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

## 📚 Documentation Structure

This repository contains multiple StarRocks implementation documents:

1. **STARROCKS_IMPLEMENTATION.md** (this file)
   - Comprehensive implementation guide
   - Detailed explanations and rationale
   - Usage examples and configuration
   - Best for: Understanding the full context

2. **IMPL_CALL_HIERARCHY_NEW.md**
   - Concise call hierarchy reference
   - Quick lookup for method relationships
   - ASCII diagram format
   - Best for: Quick reference during coding

3. **starrocks_design.md**
   - Design decisions and architecture
   - Comparison with Doris
   - Technical deep-dive
   - Best for: Understanding why decisions were made

### Quick Navigation

- Need to understand a method call flow? → See IMPL_CALL_HIERARCHY_NEW.md
- Need to implement a feature? → Use this file (STARROCKS_IMPLEMENTATION.md)
- Need to understand design rationale? → See starrocks_design.md

---

## 🔄 Changelog

### 2024-11-26 - Major Update

**Changes**:

- ✅ Merged latest insights from IMPL_CALL_HIERARCHY_NEW.md
- ✅ Corrected create_schema()/drop_schema() - NO override needed
- ✅ Clarified StarRocks is more SQL-standard than Doris
- ✅ Simplified hierarchy documentation
- ✅ Removed outdated/incorrect sections
- ✅ Updated all line number references

**Key Corrections**:

1. StarRocks supports both CREATE SCHEMA and CREATE DATABASE (use base class)
2. Only 5 core overrides needed (not 7 like initially thought)
3. Column ordering is the critical unique requirement

### 2024-11-20 - Initial Version

- Initial comprehensive implementation guide
- Based on Doris adapter analysis
- Documented all required overrides

---

## 👥 Contributors & References

**Implementation**:

- Based on Doris adapter by SQLMesh team
- StarRocks-specific adaptations: Community contributors
- Testing: In progress

**Important Notes**:

1. **PRIMARY KEY Constraint**: StarRocks PRIMARY KEY tables require partition columns to be in the primary key
2. **DELETE Performance**: Primary Key tables support efficient DELETE by primary key
3. **Materialized Views**: StarRocks 3.3+ has enhanced MV capabilities (text-based rewrite, view-based MV)
4. **Warehouse Feature**: StarRocks 3.3+ introduces warehouse concept for resource isolation

**References**:

- [StarRocks Documentation](https://docs.starrocks.io/)
- [StarRocks vs Doris Comparison](https://forum.starrocks.io/t/faq-apache-doris-vs-starrocks/128)
- [StarRocks 3.3 Release Notes](https://docs.starrocks.io/releasenotes/release-3.3/)
- [SQLMesh Engine Adapter Architecture](https://sqlmesh.readthedocs.io/)
