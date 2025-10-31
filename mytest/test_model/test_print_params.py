"""
Direct test - manually trigger create_table to print parameters
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
        description='Print model parameters passed to StarRocksEngineAdapter'
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


def format_param(value, indent=0):
    """Format parameter value"""
    prefix = "  " * indent

    if value is None:
        return f"{prefix}None"

    if isinstance(value, exp.Expression):
        type_name = type(value).__name__
        try:
            sql = value.sql(dialect="starrocks")
            return f"{prefix}{type_name}: {sql}"
        except:
            return f"{prefix}{type_name}: <cannot generate SQL>"

    elif isinstance(value, dict):
        lines = [f"{prefix}Dict ({len(value)} keys):"]
        for k, v in value.items():
            lines.append(f"{prefix}  {k}:")
            lines.append(format_param(v, indent + 2))
        return "\n".join(lines)

    elif isinstance(value, (list, tuple)):
        lines = [f"{prefix}{type(value).__name__} ({len(value)} items):"]
        for i, item in enumerate(value):
            lines.append(f"{prefix}  [{i}]:")
            lines.append(format_param(item, indent + 2))
        return "\n".join(lines)

    else:
        return f"{prefix}{type(value).__name__}: {repr(value)}"


def main():
    args = parse_args()
    
    print("=" * 100)
    print("  Direct Parameter Print Test")
    print("=" * 100)

    test_dir = Path(__file__).parent
    context = Context(paths=[str(test_dir)])
    
    # Enable SQL logging by setting adapter's execute_log_level to INFO
    if hasattr(context, '_engine_adapter') and context._engine_adapter:
        context._engine_adapter = context._engine_adapter.with_settings(
            execute_log_level=logging.INFO
        )
    
    print(f"\n✓ Loaded {len(context.models)} models\n")

    # Filter models if specific model requested
    if args.model:
        full_model_name = get_model_name(args.model)
        if full_model_name in context.models:
            models_to_test = {full_model_name: context.models[full_model_name]}
            print(f"Testing model: {args.model} -> {full_model_name}\n")
        else:
            print(f"✗ Model '{full_model_name}' not found!")
            print(f"Available models: {list(context.models.keys())}")
            print(f"\nShort names: comprehensive, complex_part, range_part, unknown_func")
            return
    else:
        models_to_test = context.models
        print(f"Testing all {len(models_to_test)} models\n")

    # Test each model
    for model_name, model in models_to_test.items():
        print("\n" + "=" * 100)
        print(f"  Model: {model_name}")
        print("=" * 100)

        print("\n--- Parameters that would be passed to _create_table_from_columns() ---\n")

        # Simulate what SQLMesh does when creating a table
        params = {
            'table_name': model.fqn,
            'target_columns_to_types': model.columns_to_types,
            'partitioned_by': model.partitioned_by_,
            'clustered_by': model.clustered_by,
            'table_properties': model.physical_properties,
            'table_description': model.description,
            'storage_format': model.storage_format,
            'primary_key': None,  # StarRocks models don't set this directly
            'exists': True,
            'column_descriptions': model.column_descriptions,
        }

        for param_name, param_value in params.items():
            if param_value:  # Only show non-empty parameters
                print(f"\n{param_name}:")
                print(format_param(param_value, indent=1))

        # Show what distributed_by looks like in detail
        if model.physical_properties and 'distributed_by' in model.physical_properties:
            print("\n" + "-" * 100)
            print("  Detailed view of 'distributed_by' structure:")
            print("-" * 100)

            dist_by = model.physical_properties['distributed_by']
            print(f"\nType: {type(dist_by).__name__}")

            if isinstance(dist_by, exp.Tuple):
                print(f"  expressions: {len(dist_by.expressions)} items")
                for i, expr in enumerate(dist_by.expressions):
                    print(f"\n  [{i}] {type(expr).__name__}:")
                    if isinstance(expr, exp.EQ):
                        left = expr.this
                        right = expr.expression
                        print(f"    Left (this): {type(left).__name__}")
                        if hasattr(left, 'this'):
                            print(f"      Value: {left.this}")
                        print(f"    Right (expression): {type(right).__name__}")
                        if isinstance(right, exp.Literal):
                            print(f"      Value: {right.this}")
                        elif isinstance(right, exp.Column):
                            print(f"      Column: {right.name}")
                        elif hasattr(right, 'sql'):
                            print(f"      SQL: {right.sql(dialect='starrocks')}")
                    print(f"    SQL: {expr.sql(dialect='starrocks')}")


if __name__ == "__main__":
    main()

    print("\n" + "=" * 100)
    print("  Summary")
    print("=" * 100)
    print("""
Key observations:

1. distributed_by structure:
   - It's a Tuple containing EQ expressions
   - Each EQ is like: Column('kind') = Literal('HASH')
   - This matches Doris adapter pattern

2. partitioned_by supports complex expressions:
   - Simple: [Column('event_date')]
   - Complex: [TimestampTrunc(Column('event_date')), Column('customer_id')]

3. physical_properties becomes table_properties parameter:
   - All keys are preserved as-is
   - Values are Expression objects

4. The adapter's _build_table_properties_exp() needs to:
   - Parse distributed_by Tuple into DistributedByProperty
   - Convert partitioned_by into PartitionByRangeProperty
   - Handle other properties like replication_num
""")
