"""
Direct test by modifying StarRocksEngineAdapter to print actual parameters.

This test directly patches the adapter methods to see EXACTLY what SQLMesh passes,
without any manual parameter construction.
"""

import sys
from pathlib import Path

# Add paths
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir / 'sqlglot'))
sys.path.insert(0, str(base_dir / 'sqlmesh'))

from sqlmesh import Context
from sqlglot import exp
import json


def format_expression(expr, indent=0):
    """Format expression for display"""
    prefix = "  " * indent

    if expr is None:
        return f"{prefix}None"

    if isinstance(expr, exp.Expression):
        lines = [f"{prefix}{type(expr).__name__}"]
        try:
            sql = expr.sql(dialect="starrocks")
            lines.append(f"{prefix}  SQL: {sql}")
        except:
            pass
        return "\n".join(lines)

    elif isinstance(expr, dict):
        lines = [f"{prefix}Dict with {len(expr)} keys:"]
        for key, value in expr.items():
            lines.append(f"{prefix}  '{key}':")
            lines.append(format_expression(value, indent + 2))
        return "\n".join(lines)

    elif isinstance(expr, (list, tuple)):
        type_name = type(expr).__name__
        lines = [f"{prefix}{type_name} with {len(expr)} items:"]
        for i, item in enumerate(expr[:10]):  # Show first 10
            lines.append(f"{prefix}  [{i}]:")
            lines.append(format_expression(item, indent + 2))
        if len(expr) > 10:
            lines.append(f"{prefix}  ... and {len(expr) - 10} more items")
        return "\n".join(lines)

    else:
        return f"{prefix}{type(expr).__name__}: {repr(expr)}"


def main():
    print("=" * 100)
    print("  Direct StarRocksEngineAdapter Parameter Test")
    print("=" * 100)

    # Patch the adapter before creating context
    from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter

    original_create_from_columns = StarRocksEngineAdapter._create_table_from_columns

    def debug_create_table_from_columns(self, **kwargs):
        """Intercept and print all parameters"""
        print("\n" + "=" * 100)
        print("  _create_table_from_columns() called with following parameters:")
        print("=" * 100)

        for param_name in sorted(kwargs.keys()):
            print(f"\n{'─' * 100}")
            print(f"Parameter: {param_name}")
            print(f"{'─' * 100}")
            param_value = kwargs[param_name]
            print(format_expression(param_value))

        print("\n" + "=" * 100)
        print("  End of parameters")
        print("=" * 100)

        # Call original method
        return original_create_from_columns(self, **kwargs)

    # Monkey patch
    StarRocksEngineAdapter._create_table_from_columns = debug_create_table_from_columns

    # Now create context - this will trigger model loading
    test_dir = Path(__file__).parent
    print(f"\nLoading context from: {test_dir}")

    context = Context(paths=[str(test_dir)])
    print(f"\n✓ Context loaded with {len(context.models)} model(s)")

    # List models
    print("\nModels found:")
    for model_name in context.models.keys():
        print(f"  - {model_name}")

    # Now try to plan - this will trigger table creation
    print("\n" + "=" * 100)
    print("  Creating plan to trigger table creation...")
    print("=" * 100)

    try:
        # This will cause SQLMesh to try to create tables
        plan = context.plan(auto_apply=False, no_prompts=True)
        print("\n✓ Plan created (parameters should have been printed above)")
    except Exception as e:
        print(f"\n✗ Error during plan: {e}")
        print("  (This is expected if StarRocks is not actually running)")
        print("  The important part is the parameter dumps above")


if __name__ == "__main__":
    main()

    print("\n" + "=" * 100)
    print("  Test Complete")
    print("=" * 100)
    print("\nNote: This test shows EXACTLY what SQLMesh passes to the adapter,")
    print("without any manual parameter construction in test code.")
