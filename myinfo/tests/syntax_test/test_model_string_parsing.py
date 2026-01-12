"""Test to verify how model parsing handles string properties."""

from sqlglot import exp, parse_one
import sqlglot

print("=" * 80)
print("Test: Parsing physical_properties value")
print("=" * 80)

# Test different physical_properties value forms
test_cases = [
    ('order_by = "id, timestamp"', "order_by"),
    ('primary_key = "id, dt"', "primary_key"),
    ('partitioned_by = "dt, region"', "partitioned_by"),
    ("order_by = id", "order_by"),
    ("order_by = (id, timestamp)", "order_by"),
]

for test_expr, prop_name in test_cases:
    print(f"\n{'='*60}")
    print(f"Testing: {test_expr}")
    print(f"{'='*60}")

    # Parse the expression
    parsed = parse_one(test_expr, dialect="starrocks")
    print(f"Parsed type: {type(parsed).__name__}")
    print(f"Parsed: {parsed}")

    if isinstance(parsed, exp.EQ):
        value = parsed.expression
        print(f"\nValue: {value}")
        print(f"Value type: {type(value).__name__}")

        if isinstance(value, exp.Literal):
            print(f"  is_string: {value.is_string}")
            print(f"  this: '{value.this}'")
        elif isinstance(value, exp.Column):
            print(f"  Column name: {value.name}")
        elif isinstance(value, exp.Tuple):
            print(f"  Tuple expressions: {value.expressions}")
            print(f"  Elements: {[type(e).__name__ for e in value.expressions]}")
