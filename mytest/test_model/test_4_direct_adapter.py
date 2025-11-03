"""
Direct test by modifying StarRocksEngineAdapter to print actual parameters.

This test directly patches the adapter methods to see EXACTLY what SQLMesh passes,
without any manual parameter construction.
"""

import sys
from pathlib import Path
import logging
import argparse

# Configure logging to see SQL statements
# Set root logger to INFO
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

# Specifically enable SQL execution logging for the engine adapter module
logging.getLogger('sqlmesh.core.engine_adapter.base').setLevel(logging.INFO)

# Add paths
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir / 'sqlglot'))
sys.path.insert(0, str(base_dir / 'sqlmesh'))

from sqlmesh import Context
from sqlglot import exp
import json


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Test StarRocks adapter with actual database connection'
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
    # If it's a short name, convert it
    if short_name in model_map:
        return model_map[short_name]
    # Otherwise return as-is (assume it's a full model name)
    return short_name


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
    args = parse_args()

    print("=" * 100)
    print("  Direct StarRocksEngineAdapter Parameter Test")
    print("=" * 100)

    # Patch the adapter before creating context
    from sqlmesh.core.engine_adapter.starrocks import StarRocksEngineAdapter

    original_create_from_columns = StarRocksEngineAdapter._create_table_from_columns
    original_init = StarRocksEngineAdapter.__init__

    def patched_init(self, *args, **kwargs):
        """Patch __init__ to set execute_log_level to INFO"""
        # Force execute_log_level to INFO
        kwargs['execute_log_level'] = logging.INFO
        original_init(self, *args, **kwargs)

    # Monkey patch __init__
    StarRocksEngineAdapter.__init__ = patched_init

    def debug_create_table_from_columns(self, *args, **kwargs):
        """Intercept and print all parameters"""
        print("\n" + "=" * 100)
        print("  _create_table_from_columns() called with following parameters:")
        print("=" * 100)

        # Process positional args
        if args:
            print(f"\n{'─' * 100}")
            print("Positional arguments:")
            print(f"{'─' * 100}")
            for i, arg in enumerate(args):
                print(f"  args[{i}]: {type(arg).__name__}")

        # Process keyword args
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
        return original_create_from_columns(self, *args, **kwargs)

    # Monkey patch
    StarRocksEngineAdapter._create_table_from_columns = debug_create_table_from_columns

    # Now create context - this will trigger model loading
    test_dir = Path(__file__).parent
    print(f"\nLoading context from: {test_dir}")

    context = Context(paths=[str(test_dir)])

    # Enable SQL logging on the adapter
    if hasattr(context, '_engine_adapter') and context._engine_adapter:
        context._engine_adapter = context._engine_adapter.with_settings(
            execute_log_level=logging.INFO
        )
        print("\n✓ SQL logging enabled on adapter")

    print(f"\n✓ Context loaded with {len(context.models)} model(s)")

    # Filter models if specific model requested
    if args.model:
        full_model_name = get_model_name(args.model)
        if full_model_name in context.models:
            models_to_test = {full_model_name: context.models[full_model_name]}
            print(f"\nTesting model: {args.model} -> {full_model_name}")
        else:
            print(f"\n✗ Model '{full_model_name}' not found!")
            print("Available models:")
            for model_name in context.models.keys():
                print(f"  - {model_name}")
            print("\nShort names: comprehensive, complex_part, range_part, unknown_func")
            return
    else:
        models_to_test = context.models
        print(f"\nTesting all {len(models_to_test)} models")

    # List models
    print("\nModels found:")
    for model_name in models_to_test.keys():
        print(f"  - {model_name}")

    # Now try to plan - this will trigger table creation
    print("\n" + "=" * 100)
    print("  Creating plan to trigger table creation...")
    print("=" * 100)
    print()
    print("IMPORTANT: With auto_apply=True, SQLMesh will:")
    print("  1. Create a Plan (analyze what needs to change)")
    print("  2. Apply the Plan (actually create tables and load data)")
    print()
    print("Prerequisites for model execution:")
    print("  - Seed file: seeds/source_table.csv must exist")
    print("  - Database: StarRocks must be running and accessible")
    print("  - Adapter: _get_data_objects() must be implemented")
    print()

    try:
        # Use select_models to test only selected models
        if args.model:
            plan = context.plan(
                auto_apply=True,  # Will actually create tables and load data
                no_prompts=True,
                select_models=[full_model_name]
            )
        else:
            plan = context.plan(auto_apply=True, no_prompts=True)
        print("\n✓ Plan created and applied successfully!")
        print()
        print("✓ Tables should now exist in the database")
        print("✓ Data should be loaded from seed file")
        print()
        print("You can verify by running:")
        print(f"  SELECT * FROM mytest.{args.model if args.model else 'starrocks_comprehensive'};")
    except Exception as e:
        print(f"\n✗ Error during plan: {e!r}")
        print("  (This is expected if StarRocks is not actually running)")
        print("  The important part is the parameter dumps above")

        # Print detailed traceback
        import traceback
        print("\n" + "="*100)
        print("  Detailed Traceback:")
        print("="*100)
        traceback.print_exc()

        # Print exception details
        print("\n" + "="*100)
        print("  Exception Details:")
        print("="*100)
        print(f"  Type: {type(e).__name__}")
        print(f"  Args: {e.args}")
        if hasattr(e, '__dict__'):
            print(f"  Attributes: {e.__dict__}")


if __name__ == "__main__":
    main()

    print("\n" + "=" * 100)
    print("  Test Complete")
    print("=" * 100)
    print("\nNote: This test shows EXACTLY what SQLMesh passes to the adapter,")
    print("without any manual parameter construction in test code.")
