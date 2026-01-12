"""
Test to understand how SQLMesh parses model properties

This test shows:
1. What properties are defined in the model file
2. How they are parsed by SQLMesh
3. What structure they have internally
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

# Add paths for sqlglot and sqlmesh
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir / 'sqlglot'))
sys.path.insert(0, str(base_dir / 'sqlmesh'))

try:
    from sqlmesh import Context
    from pprint import pprint
except ImportError as e:
    print(f"Import error: {e}")
    print(f"sys.path: {sys.path[:5]}")
    sys.exit(1)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='SQLMesh Model Property Parsing Test'
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


def main():
    args = parse_args()

    print("=" * 80)
    print("  SQLMesh Model Property Parsing Test")
    print("=" * 80)

    # Get working directory
    mytest_dir = Path(__file__).parent
    print(f"\nWorking directory: {mytest_dir}")

    # Create context
    print("\n1. Creating SQLMesh Context...")
    try:
        context = Context(paths=[str(mytest_dir)])

        # Enable SQL logging
        if hasattr(context, '_engine_adapter') and context._engine_adapter:
            context._engine_adapter = context._engine_adapter.with_settings(
                execute_log_level=logging.INFO
            )

        print(f"   ✓ Context created")
        print(f"   - Default dialect: {context.config.dialect}")
        print(f"   - Models found: {len(context.models)}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return

    # Find our model
    if args.model:
        model_name = get_model_name(args.model)
        print(f"\n2. Loading model: {args.model} -> {model_name}")
    else:
        model_name = '"mytest"."starrocks_comprehensive"'
        print(f"\n2. Loading model: {model_name} (default)")
        print(f"   Tip: Use -m comprehensive, complex_part, range_part, or unknown_func")

    if model_name not in context.models:
        print(f"   ✗ Model not found!")
        print(f"   Available models: {list(context.models.keys())}")
        return

    model = context.models[model_name]
    print(f"   ✓ Model loaded: {model.name}")
    print(f"   - Type: {type(model).__name__}")
    print(f"   - Kind: {model.kind}")

    # Print all model attributes
    print("\n3. Model Attributes:")
    print("-" * 80)

    attrs_to_check = [
        'name', 'dialect', 'owner', 'kind', 'cron', 'start',
        'partitioned_by_', 'clustered_by', 'storage_format', 'table_format',
        'columns_to_types', 'annotated_columns'
    ]

    for attr in attrs_to_check:
        if hasattr(model, attr):
            value = getattr(model, attr)
            print(f"\n{attr}:")

            if attr == 'columns_to_types' and value:
                for col_name, col_type in value.items():
                    print(f"  - {col_name}: {col_type}")
            elif attr in ['partitioned_by_', 'clustered_by', 'annotated_columns'] and value:
                for i, item in enumerate(value):
                    print(f"  [{i}] {item} (type: {type(item).__name__})")
            else:
                print(f"  {value}")

    # Check for physical_properties / table_properties
    print("\n4. Physical Properties:")
    print("-" * 80)

    # Try different attribute names
    for attr_name in ['table_properties', 'physical_properties', '_table_properties']:
        if hasattr(model, attr_name):
            value = getattr(model, attr_name)
            print(f"\n{attr_name}:")
            if isinstance(value, dict):
                for k, v in value.items():
                    print(f"  {k}: {v} (type: {type(v).__name__})")
            else:
                print(f"  {value} (type: {type(value).__name__})")

    # Print model's __dict__ to see all available attributes
    print("\n5. All Model Attributes (from __dict__):")
    print("-" * 80)
    excluded_keys = {'_query', 'query', 'python_env', '_full_depends_on'}
    for key in sorted(model.__dict__.keys()):
        if key not in excluded_keys and not key.startswith('__'):
            value = model.__dict__[key]
            if value is not None and value != '' and value != [] and value != {}:
                print(f"  {key}: {type(value).__name__}")
                if isinstance(value, (str, int, bool)):
                    print(f"    = {value}")


if __name__ == "__main__":
    main()

    print("\n" + "=" * 80)
    print("  TEST COMPLETE")
    print("=" * 80)
