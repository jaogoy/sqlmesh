"""
Detailed structure analysis for RANGE(col1, col2) BUCKETS syntax
"""
import sys
import os

from sqlmesh.core.model.common import parse_expression
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlmesh.core.model.definition import SqlModel

try:
    from sqlglot import parse_one
    from sqlglot.expressions import Expression
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "sqlglot"])
    from sqlglot import parse_one
    from sqlglot.expressions import Expression

import json


def analyze_expression(expr: Expression, depth=0) -> dict:
    """Recursively analyze expression structure"""
    indent = "  " * depth

    result = {
        "type": type(expr).__name__,
        "class": str(expr.__class__),
        "sql": expr.sql() if hasattr(expr, 'sql') else str(expr),
        "key": expr.key if hasattr(expr, 'key') else None,
        "args": {}
    }

    # Analyze arguments
    if hasattr(expr, 'args') and expr.args:
        for key, value in expr.args.items():
            if isinstance(value, list):
                result["args"][key] = [
                    analyze_expression(item, depth + 1) if hasattr(item, 'sql')
                    else {"type": type(item).__name__, "value": str(item)}
                    for item in value
                ]
            elif value is not None:
                if hasattr(value, 'sql'):
                    result["args"][key] = analyze_expression(value, depth + 1)
                else:
                    result["args"][key] = {"type": type(value).__name__, "value": str(value)}

    return result


def print_structure(data, indent=0):
    """Pretty print the structure"""
    prefix = "  " * indent

    if isinstance(data, dict):
        for key, value in data.items():
            if key in ['type', 'sql', 'key']:
                print(f"{prefix}{key}: {value}")
            elif key == 'args' and value:
                print(f"{prefix}{key}:")
                print_structure(value, indent + 1)
            elif isinstance(value, (dict, list)):
                print(f"{prefix}{key}:")
                print_structure(value, indent + 1)
            else:
                print(f"{prefix}{key}: {value}")
    elif isinstance(data, list):
        for i, item in enumerate(data):
            print(f"{prefix}[{i}]:")
            print_structure(item, indent + 1)


def test_expression(expr_str, description):
    """Test a single expression"""
    print("\n" + "=" * 80)
    print(f"Testing: {description}")
    print("=" * 80)
    print(f"Expression: {expr_str}")
    print("-" * 80)

    try:
        parsed = parse_one(expr_str)
        print(f"✓ Parsing successful!")
        print(f"\nType: {type(parsed).__name__}")
        print(f"SQL output: {parsed.sql()}")
        print(f"\n📊 Detailed Structure:")
        print("-" * 80)

        structure = analyze_expression(parsed)
        print_structure(structure)

        # Additional analysis
        print(f"\n🔍 Key Properties:")
        print(f"  - Expression key: {parsed.key}")
        print(f"  - Has 'this': {'this' in parsed.args if hasattr(parsed, 'args') else False}")
        print(f"  - Has 'expressions': {'expressions' in parsed.args if hasattr(parsed, 'args') else False}")
        print(f"  - Has 'alias': {'alias' in parsed.args if hasattr(parsed, 'args') else False}")

        return True, structure

    except Exception as e:
        print(f"✗ Parsing failed!")
        print(f"Error: {e}")
        return False, None


def test_partition_clause():
    print("=" * 80)
    print("SQLGlot Expression Structure Analysis")
    print("Testing RANGE(col1, col2) BUCKETS syntax")
    print("=" * 80)

    tests = [
        ("RANGE(col1, col2)", "RANGE function only"),
        ("RANGE(col1, col2) BUCKETS", "RANGE with BUCKETS keyword"),
        ("RANGE(col1, col2) BUCKETS 10", "RANGE with BUCKETS and count"),
        ("PARTITIONED BY RANGE(col1, col2) BUCKETS 10", "with PARTITION BY"),
        ("col1", "Single column"),
        ("RANGE(col1)", "RANGE with single column"),
        ("BUCKETS 10", "Buckets only"),
    ]

    results = {}
    for expr, desc in tests:
        success, structure = test_expression(expr, desc)
        results[expr] = {
            "success": success,
            "structure": structure
        }

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY OF RESULTS")
    print("=" * 80)

    for expr, result in results.items():
        status = "✓" if result["success"] else "✗"
        expr_type = result["structure"]["type"] if result["structure"] else "N/A"
        print(f"{status} {expr:40} -> {expr_type}")

    # Key findings
    print("\n" + "=" * 80)
    print("KEY FINDINGS")
    print("=" * 80)
    print("""
1. RANGE(col1, col2) alone:
   - Parsed as: Anonymous function
   - Can be used in expressions

2. RANGE(col1, col2) BUCKETS:
   - Parsed as: Alias (INCORRECT!)
   - SQLGlot treats BUCKETS as an alias name
   - This is NOT the intended partition syntax

3. RANGE(col1, col2) BUCKETS 10:
   - Cannot be parsed by SQLGlot
   - Raises ParseError
   - This syntax is NOT supported

CONCLUSION:
The syntax "RANGE(col1, col2) BUCKETS 10" cannot be directly parsed
by SQLGlot or SQLMesh. You need to either:
- Extend SQLGlot's parser to support this syntax
- Use string manipulation to extract components
- Handle it as a special case in model definitions
    """)


def test_partition_special_cases():
    partitioned_by = "(RANGE(col1, col2) BUCKETS 10)"
    parsed = parse_expression(SqlModel, partitioned_by, None)
    print(parsed)
    structure = analyze_expression(parsed)
    print_structure(structure)


def main():
    test_partition_clause()
    # test_partition_special_cases()

if __name__ == "__main__":
    main()
