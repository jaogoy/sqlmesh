# SQLGlot Parsing Test Results for `RANGE(col1, col2) BUCKETS 10`

## Test Results Summary

### ✅ Test 1: `RANGE(col1, col2)` alone
**Status:** Success
**Parsed as:** `Anonymous` function call
**Structure:**
```python
Anonymous(
    this='RANGE',
    expressions=[
        Column(this=Identifier(this=col1, quoted=False)),
        Column(this=Identifier(this=col2, quoted=False))
    ]
)
```

### ⚠️ Test 2: `RANGE(col1, col2) BUCKETS`
**Status:** Incorrectly parsed
**Parsed as:** `Alias` expression (NOT a partition clause)
**Structure:**
```python
Alias(
    this=Anonymous(
        this='RANGE',
        expressions=[
            Column(this=Identifier(this=col1, quoted=False)),
            Column(this=Identifier(this=col2, quoted=False))
        ]
    ),
    alias=Identifier(this='BUCKETS', quoted=False)
)
```
**Note:** SQLGlot treats "BUCKETS" as an alias name, like `RANGE(col1, col2) AS BUCKETS`

### ❌ Test 3: `RANGE(col1, col2) BUCKETS 10`
**Status:** Parse Error
**Error:** `Invalid expression / Unexpected token. Line 1, Col: 28`
**Reason:** SQLGlot cannot understand this syntax

### ❌ Test 4: Full CREATE TABLE with partition clause
**Status:** Parsed as `Command` (unparsed SQL)
**Parsed as:** `Command` expression
**Structure:**
```python
Command(
    this='CREATE',
    expression=' TABLE test_table (...) PARTITION BY RANGE(col1, col2) BUCKETS 10'
)
```
**Note:** SQLGlot doesn't parse it semantically - treats it as a raw command string

### ❌ Test 5: Different dialects (hive, spark, starrocks, doris)
**Status:** All failed with the same ParseError

## Conclusion

### Key Findings:
1. **`RANGE(col1, col2)`** alone is parsed as an **Anonymous function call** ✓
2. **`RANGE(col1, col2) BUCKETS`** is incorrectly parsed as an **Alias expression** ⚠️
3. **`RANGE(col1, col2) BUCKETS 10`** causes a **ParseError** ❌
4. SQLGlot does **NOT** natively support this partition bucket syntax in any dialect

### Recommendations:
1. **For SQLMesh/SQLGlot usage:**
   - This syntax is NOT directly parsable as a standalone expression
   - You need to extend SQLGlot's parser to support this syntax
   - Or handle it as a special case in your model definition

2. **Alternative approaches:**
   - Parse it as part of a full DDL statement (though it's still treated as a Command)
   - Write custom parsing logic to extract RANGE columns and BUCKETS count
   - Use string manipulation to extract the components

3. **For StarRocks/Doris support:**
   - You'll need to add custom grammar rules to SQLGlot
   - Extend the parser to recognize `BUCKETS <number>` after partition expressions
   - Create a new Expression type for bucketed partitions

## Expression Type Details

| Expression | Type | SQL Output |
|------------|------|------------|
| `RANGE(col1, col2)` | `Anonymous` | `RANGE(col1, col2)` |
| `RANGE(col1, col2) BUCKETS` | `Alias` | `RANGE(col1, col2) AS BUCKETS` |
| `RANGE(col1, col2) BUCKETS 10` | Error | N/A |
| Full CREATE TABLE | `Command` | Raw string |
