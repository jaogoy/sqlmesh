import sys
import os

# Add the parent directory to the path to import sqlmesh
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from sqlglot import parse_one
    from sqlmesh.core.model import SqlModel
    from sqlmesh.core.model.common import parse_expression
except ImportError as e:
    print(f"Import error: {e}")
    print("\nTrying to install required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "sqlglot"])
    from sqlglot import parse_one

import json

def print_expression_details(expr, indent=0):
    """Recursively print expression details"""
    prefix = "  " * indent
    print(f"{prefix}Type: {type(expr).__name__}")
    print(f"{prefix}Class: {expr.__class__}")
    print(f"{prefix}SQL: {expr.sql()}")
    print(f"{prefix}Key: {expr.key if hasattr(expr, 'key') else 'N/A'}")

    # Print args summary
    if hasattr(expr, 'args') and expr.args:
        print(f"{prefix}Args keys: {list(expr.args.keys())}")

    # Print child expressions (limited depth for readability)
    if expr.args and indent < 2:
        print(f"{prefix}Children:")
        for key, value in expr.args.items():
            print(f"{prefix}  Key '{key}':")
            if isinstance(value, list):
                for i, item in enumerate(value[:3]):  # Limit to first 3 items
                    print(f"{prefix}    [{i}]: {type(item).__name__}")
                    if hasattr(item, 'sql'):
                        print(f"{prefix}        SQL: {item.sql()}")
            elif value is not None:
                if hasattr(value, 'sql'):
                    print(f"{prefix}    Type: {type(value).__name__}")
                    print(f"{prefix}    SQL: {value.sql()}")
                else:
                    print(f"{prefix}    {type(value).__name__}: {value}")

def test_parse_expression():
    print("=" * 80)
    print("SQL Syntax Parsing Tests")
    print("=" * 80)

    # Test 1: Full partition syntax with CREATE TABLE
    print("\n" + "=" * 80)
    print("Test 1: Full CREATE TABLE with partition clause")
    print("=" * 80)

    sql1 = """
    CREATE TABLE test_table (
        col1 INT,
        col2 STRING
    )
    PARTITION BY RANGE(col1, col2) BUCKETS 10
    """

    print("\nSQL:")
    print(sql1)
    print("\nParsing...")
    try:
        val1 = parse_one(sql1)
        print(f"Result: {val1}")
        print(f"Type: {type(val1).__name__}")
        print(f"SQL: {val1.sql()}")
        print("\nDetailed structure:")
        print_expression_details(val1)
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: Just RANGE function
    print("\n" + "=" * 80)
    print("Test 2: RANGE(col1, col2) only")
    print("=" * 80)
    partition2 = "RANGE(col1, col2)"
    print(f"\nExpression: {partition2}")
    try:
        val2 = parse_one(partition2)
        print(f"Result: {val2}")
        print(f"Type: {type(val2).__name__}")
        print(f"SQL: {val2.sql()}")
        print(f"\nKey info - Expression class: {val2.__class__.__name__}")
        print(f"Arguments: {val2.args}")
    except Exception as e:
        print(f"Error: {e}")

    # Test 3: RANGE with BUCKETS (no number)
    print("\n" + "=" * 80)
    print("Test 3: RANGE(col1, col2) BUCKETS")
    print("=" * 80)
    partition3 = "RANGE(col1, col2) BUCKETS"
    print(f"\nExpression: {partition3}")
    try:
        val3 = parse_one(partition3)
        print(f"Result: {val3}")
        print(f"Type: {type(val3).__name__}")
        print(f"SQL: {val3.sql()}")
        print(f"\nKey info - Expression class: {val3.__class__.__name__}")
        print(f"Arguments: {val3.args}")
        print(f"\n⚠️ Note: This is parsed as an ALIAS, not a partition clause!")
    except Exception as e:
        print(f"Error: {e}")

    # Test 4: RANGE with BUCKETS 10
    print("\n" + "=" * 80)
    print("Test 4: RANGE(col1, col2) BUCKETS 10")
    print("=" * 80)
    partition4 = "RANGE(col1, col2) BUCKETS 10"
    print(f"\nExpression: {partition4}")
    try:
        val4 = parse_one(partition4)
        print(f"Result: {val4}")
        print(f"Type: {type(val4).__name__}")
        print(f"SQL: {val4.sql()}")
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"\n⚠️ This syntax is NOT supported by SQLGlot's default parser")

    # Test 5: Try with dialect specification
    print("\n" + "=" * 80)
    print("Test 5: Try with different dialects")
    print("=" * 80)

    for dialect in ['hive', 'spark', 'starrocks', 'doris']:
        print(f"\n--- Dialect: {dialect} ---")
        try:
            from sqlglot import parse_one as parse
            val5 = parse(partition4, dialect=dialect)
            print(f"✓ Success! Type: {type(val5).__name__}")
            print(f"  SQL: {val5.sql()}")
        except Exception as e:
            print(f"✗ Failed: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
    Findings:
    1. 'RANGE(col1, col2)' alone is parsed as an Anonymous function call
    2. 'RANGE(col1, col2) BUCKETS' is incorrectly parsed as an Alias expression
    3. 'RANGE(col1, col2) BUCKETS 10' causes a ParseError
    4. This suggests SQLGlot does not natively support this partition syntax

    Recommendation:
    - You may need to use a CREATE TABLE statement with PARTITION BY clause
    - Or extend SQLGlot's parser to support this specific syntax
    - Or use a different approach for parsing partition clauses
    """)


if __name__ == "__main__":
    test_parse_expression()
