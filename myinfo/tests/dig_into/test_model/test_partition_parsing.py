"""
Test to understand partition expression parsing

This test analyzes:
1. Why RANGE(dt) becomes exp.Anonymous
2. String vs expression parsing
3. Unknown function handling
4. AST tree structure
"""

import sys
from pathlib import Path
import argparse
import logging

# Configure logging to see SQL statements  
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(name)s - %(message)s'
)

# Add paths
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir / 'sqlglot'))
sys.path.insert(0, str(base_dir / 'sqlmesh'))

from sqlmesh import Context
from sqlglot import exp


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Partition Expression Parsing Analysis'
    )
    parser.add_argument(
        '--model',
        '-m',
        type=str,
        help='Model to test: comprehensive, complex_part, range_part, unknown_func, or full model name'
    )
    return parser.parse_args()


def get_model_name(short_name: str) -> str:
    """Convert short name to full model name"""
    model_map = {
        'comprehensive': '"mytest"."starrocks_comprehensive"',
        'complex_part': '"mytest"."starrocks_complex_partition"',
        'range_part': '"mytest"."test_range_partition"',
        'unknown_func': '"mytest"."test_unknown_func"',
    }
    if short_name in model_map:
        return model_map[short_name]
    return short_name


def analyze_expression(expr, indent=0, show_all_args=False):
    """Recursively analyze expression structure
    
    Args:
        expr: SQLGlot expression to analyze
        indent: Indentation level
        show_all_args: If True, show all expression args and nested structure
    """
    prefix = "  " * indent

    if expr is None:
        return f"{prefix}None"

    if isinstance(expr, exp.Expression):
        lines = [f"{prefix}{type(expr).__name__}"]

        # Show ALL args for detailed analysis
        if show_all_args and hasattr(expr, 'args'):
            lines.append(f"{prefix}  Available args: {list(expr.args.keys())}")

        # Show key attributes
        if hasattr(expr, 'this') and expr.this is not None:
            if isinstance(expr.this, exp.Expression):
                lines.append(f"{prefix}  this: {type(expr.this).__name__}")
                if isinstance(expr.this, exp.Identifier):
                    lines.append(f"{prefix}    identifier value: '{expr.this.this}'")
                # Recursively show this if it's complex
                elif show_all_args:
                    lines.append(f"{prefix}    Details:")
                    lines.append(analyze_expression(expr.this, indent + 2, show_all_args=True))
            elif isinstance(expr.this, str):
                lines.append(f"{prefix}  this (str): '{expr.this}'")
            else:
                lines.append(f"{prefix}  this ({type(expr.this).__name__}): {expr.this}")

        # Show name for Anonymous/functions
        if hasattr(expr, 'name'):
            lines.append(f"{prefix}  name: '{expr.name}'")

        # Show unit for TimestampTrunc/DateTrunc
        if hasattr(expr, 'args') and 'unit' in expr.args:
            unit = expr.args['unit']
            if isinstance(unit, exp.Expression):
                lines.append(f"{prefix}  unit: {type(unit).__name__} = {unit}")
            else:
                lines.append(f"{prefix}  unit: {unit}")

        # Show expressions (arguments)
        if hasattr(expr, 'expressions') and expr.expressions:
            lines.append(f"{prefix}  expressions ({len(expr.expressions)} items):")
            for i, sub_expr in enumerate(expr.expressions):
                lines.append(f"{prefix}    [{i}]:")
                lines.append(analyze_expression(sub_expr, indent + 3, show_all_args=show_all_args))

        # Show SQL representation
        try:
            sql = expr.sql(dialect='starrocks')
            lines.append(f"{prefix}  SQL: {sql}")
        except:
            pass

        return "\n".join(lines)

    else:
        return f"{prefix}{type(expr).__name__}: {repr(expr)}"


def main():
    args = parse_args()

    print("=" * 100)
    print("  Partition Expression Parsing Analysis")
    print("=" * 100)

    test_dir = Path(__file__).parent
    context = Context(paths=[str(test_dir)])
    
    # Enable SQL logging
    if hasattr(context, '_engine_adapter') and context._engine_adapter:
        context._engine_adapter = context._engine_adapter.with_settings(
            execute_log_level=logging.INFO
        )

    print(f"\n✓ Loaded {len(context.models)} models\n")

    # Filter models if requested
    if args.model:
        full_model_name = get_model_name(args.model)
        if full_model_name in context.models:
            models_to_analyze = {full_model_name: context.models[full_model_name]}
            print(f"Analyzing: {args.model} -> {full_model_name}\n")
        else:
            print(f"✗ Model '{full_model_name}' not found!")
            print(f"Available: {list(context.models.keys())}")
            return
    else:
        models_to_analyze = context.models

    # Analyze each model's partitioned_by
    for model_name, model in sorted(models_to_analyze.items()):
        if not hasattr(model, 'partitioned_by_'):
            continue

        print("\n" + "=" * 100)
        print(f"  Model: {model_name}")
        print("=" * 100)

        print(f"\npartitioned_by_ type: {type(model.partitioned_by_)}")
        print(f"partitioned_by_ length: {len(model.partitioned_by_)}")

        for i, expr in enumerate(model.partitioned_by_):
            print(f"\n{'─' * 100}")
            print(f"Expression [{i}]:")
            print(f"{'─' * 100}")
            
            # Show basic analysis first
            print(analyze_expression(expr))
            
            # Show detailed analysis for complex expressions
            if isinstance(expr, (exp.TimestampTrunc, exp.DateTrunc, exp.Anonymous)):
                print(f"\n{'─' * 100}")
                print(f"DETAILED ANALYSIS for {type(expr).__name__}:")
                print(f"{'─' * 100}")
                print(analyze_expression(expr, show_all_args=True))

    # Deep dive into specific cases
    print("\n" + "=" * 100)
    print("  Deep Analysis: RANGE(dt) parsing")
    print("=" * 100)

    # Test direct parsing
    from sqlglot import parse_one

    test_cases = [
        ("RANGE(dt)", "Function call - recognized"),
        ("date_trunc('day', dt)", "Known function"),
        ("my_custom_func('day', dt)", "Unknown function"),
        ("RANGE(a, b)", "Multiple arguments"),
    ]

    for sql, description in test_cases:
        print(f"\n{'─' * 100}")
        print(f"Test: {sql}")
        print(f"Description: {description}")
        print(f"{'─' * 100}")

        try:
            parsed = parse_one(sql, dialect='starrocks')
            print(analyze_expression(parsed))
        except Exception as e:
            print(f"Parse error: {e}")

    # Explain the flow
    print("\n" + "=" * 100)
    print("  Understanding 'this' in SQLGlot Expressions")
    print("=" * 100)

    print("""
In SQLGlot, every Expression has a consistent structure:

exp.Expression(
    this=<main_subject>,      # The primary element the expression operates on
    expressions=[...],        # Additional arguments/operands
    **other_args             # Type-specific properties
)

### What does 'this' represent?

'this' is the PRIMARY SUBJECT of an expression:

1. **For Functions (e.g., TimestampTrunc)**:
   ```python
   DATE_TRUNC('DAY', event_date)
   ↓
   TimestampTrunc(
       this=Column('event_date'),  # The column being truncated
       unit=Var('DAY')              # The truncation unit (in args)
   )
   ```
   → 'this' = what you're operating on (the column)

2. **For Anonymous (unknown functions)**:
   ```python
   RANGE(dt)
   ↓  
   Anonymous(
       this='RANGE',               # The function NAME (string)
       expressions=[Column('dt')]  # The arguments
   )
   ```
   → 'this' = function name (because SQLGlot doesn't know this function)

3. **For Binary Operations (e.g., EQ)**:
   ```python
   kind = 'HASH'
   ↓
   EQ(
       this=Column('kind'),        # Left side
       expression=Literal('HASH')  # Right side
   )
   ```
   → 'this' = left operand

4. **For Column**:
   ```python
   event_date
   ↓
   Column(
       this=Identifier('event_date')  # The identifier object
   )
   ```
   → 'this' = the identifier itself

### Why TimestampTrunc.this is Column?

Because TimestampTrunc is defined as:
```python
class TimestampTrunc(Func):
    # Signature: TIMESTAMP_TRUNC(column, unit)
    # 'this' = the column/expression to truncate
    # 'unit' = stored in args['unit']
```

SQLGlot's design: First positional arg goes to 'this', rest go to 'expressions' or named args.

### Why Anonymous.this is a string 'RANGE'?

Because Anonymous represents UNKNOWN syntax:
```python
class Anonymous(Func):
    # 'this' = the function name (as string)
    # 'expressions' = the arguments
```

Since SQLGlot doesn't have a RANGE function class, it stores:
- The name as a string in 'this'
- Arguments in 'expressions'

This allows round-trip: Anonymous('RANGE', [Column('dt')]) → "RANGE(dt)"
""")

    print("\n" + "=" * 100)
    print("  Parsing Flow Explanation")
    print("=" * 100)

    print("""
1. MODEL partitioned_by Parsing:
   ────────────────────────────────
   partitioned_by RANGE(dt)
        ↓
   SQLMesh dialect parser calls _parse_partitioned_by()
        ↓
   SQLGlot standard parser interprets RANGE as function-like token
        ↓
   RANGE is NOT a known SQLGlot function → exp.Anonymous(this='RANGE', expressions=[Column('dt')])
        ↓
   Wrapped in PartitionedByProperty → Schema → List extraction
        ↓
   model.partitioned_by_ = [Anonymous(this='RANGE', expressions=[Column('dt')])]

2. Why Anonymous and not a Function?
   ────────────────────────────────
   - SQLGlot has specific function classes: DateTrunc, TimestampTrunc, etc.
   - RANGE is not registered as a function in SQLGlot
   - Unknown function-like syntax → exp.Anonymous
   - This is a FALLBACK mechanism for extensibility

3. String vs Expression:
   ────────────────────────────────
   partitioned_by "RANGE(dt)"  → Literal string (no parsing)
   partitioned_by RANGE(dt)    → Parsed as expression tree

4. Unknown Functions:
   ────────────────────────────────
   my_custom_func('day', dt) → exp.Anonymous(this='my_custom_func', expressions=[...])

   SQLGlot parser:
   - Sees identifier followed by (...)
   - Checks if it's a registered function
   - If not → creates Anonymous with that name
   - Arguments still parsed into expressions list

5. AST Tree Transformation:
   ────────────────────────────────
   MODEL Definition (SQLMesh syntax)
        ↓
   SQLMesh Parser (dialect.py) - creates MODEL-specific AST
        ├─ partitioned_by → calls SQLGlot parser
        ├─ distributed_by → generic field parser
        └─ columns → schema parser
        ↓
   Model Object (Python) - SQLGlot expressions
        ↓
   EngineAdapter receives expressions
        ↓
   Adapter transforms to dialect-specific AST
        ├─ Parse Anonymous('RANGE') → PartitionByRangeProperty
        ├─ Parse distributed_by Tuple → DistributedByProperty
        └─ Build final Properties AST
        ↓
   SQLGlot Generator renders to SQL
        ↓
   CREATE TABLE ... PARTITION BY RANGE(dt) ... DISTRIBUTED BY HASH(id) BUCKETS 10

6. Why Doris Parses "RANGE(a, b)" String:
   ────────────────────────────────────────
   Because users might write:
   - partitioned_by "RANGE(dt)"  (string literal)
   - partitions = ('PARTITION p1 VALUES LESS THAN ("2024-01-01")', ...)  (strings)

   Adapter needs to handle BOTH:
   - Expression trees: Anonymous('RANGE', [Column('dt')])
   - String literals: Literal("RANGE(dt)")

   String parsing is a FALLBACK for backward compatibility and user convenience.

7. Your Understanding is CORRECT:
   ─────────────────────────────────
   ✓ Non-string values → parsed into detailed Expression tree
   ✓ Unknown tokens → exp.Anonymous
   ✓ RANGE(a, b) → Anonymous('RANGE', expressions=[Column('a'), Column('b')])
   ✓ Two-phase transformation:
     • SQLMesh parsing → MODEL syntax AST (with embedded SQLGlot expressions)
     • Adapter transformation → Dialect-specific SQLGlot AST
""")


if __name__ == "__main__":
    main()

    print("\n" + "=" * 100)
    print("  Key Insights")
    print("=" * 100)
    print("""
1. exp.Anonymous is SQLGlot's EXTENSION POINT for unknown functions
2. String literals bypass parsing - used for raw SQL snippets
3. SQLMesh always tries to parse non-strings into expression trees
4. Adapter's job: Transform generic expressions → dialect-specific AST
5. Two AST levels: SQLMesh MODEL syntax tree + SQLGlot SQL expression tree
""")
