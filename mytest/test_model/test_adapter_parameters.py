"""
Test to see how model properties are passed to StarRocksEngineAdapter.create_table()

This demonstrates the full flow:
1. Model definition in SQL
2. Parsed properties in SQLMesh
3. Parameters passed to engine adapter
4. Final SQL generation
"""

import sys
from pathlib import Path
from unittest.mock import patch
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
        description='Test model properties → engine adapter parameter flow'
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


def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def format_value(value, indent=0):
    """Format a value for pretty printing"""
    prefix = "  " * indent

    if isinstance(value, exp.Expression):
        try:
            sql = value.sql(dialect="starrocks")
            return f"{type(value).__name__}: {sql}"
        except:
            return f"{type(value).__name__}"

    elif isinstance(value, dict):
        lines = [f"dict ({len(value)} keys):"]
        for k, v in value.items():
            lines.append(f"  {k}: {format_value(v)}")
        return "\n".join(lines)

    elif isinstance(value, (list, tuple)):
        if not value:
            return f"{type(value).__name__} (empty)"
        lines = [f"{type(value).__name__} ({len(value)} items):"]
        for i, item in enumerate(value[:5]):
            lines.append(f"  [{i}] {format_value(item)}")
        if len(value) > 5:
            lines.append(f"  ... and {len(value) - 5} more")
        return "\n".join(lines)

    else:
        return f"{type(value).__name__}: {value}"


def main():
    args = parse_args()
    
    print_section("SQLMesh Model → Engine Adapter Parameter Flow")

    # Setup
    mytest_dir = Path(__file__).parent
    print(f"\nWorking directory: {mytest_dir}")

    # Create context
    print("\n1. Loading SQLMesh Context...")
    context = Context(paths=[str(mytest_dir)])
    
    # Enable SQL logging
    if hasattr(context, '_engine_adapter') and context._engine_adapter:
        context._engine_adapter = context._engine_adapter.with_settings(
            execute_log_level=logging.INFO
        )
    
    print(f"   ✓ Context loaded with {len(context.models)} model(s)")

    # Get model
    if args.model:
        full_model_name = get_model_name(args.model)
        if full_model_name in context.models:
            model_name = full_model_name
            print(f"   Testing: {args.model} -> {model_name}")
        else:
            print(f"   ✗ Model '{full_model_name}' not found!")
            print(f"   Available: {list(context.models.keys())}")
            return
    else:
        model_name = list(context.models.keys())[0]
        print(f"   Testing: {model_name} (first model, use -m to specify)")
    
    model = context.models[model_name]

    print_section("STEP 1: Model Definition Properties")

    print("\nBasic Properties:")
    print(f"  name: {model.name}")
    print(f"  kind: {model.kind}")
    print(f"  dialect: {model.dialect}")
    print(f"  owner: {model.owner}")
    print(f"  cron: {model.cron}")
    print(f"  storage_format: {model.storage_format}")

    print("\nPartitioning & Clustering:")
    print(f"  partitioned_by_:")
    for item in model.partitioned_by_:
        print(f"    - {item} (type: {type(item).__name__})")
        if isinstance(item, exp.Expression):
            print(f"      SQL: {item.sql(dialect='starrocks')}")

    print(f"\n  clustered_by:")
    for item in model.clustered_by:
        print(f"    - {item} (type: {type(item).__name__})")
        if isinstance(item, exp.Expression):
            print(f"      SQL: {item.sql(dialect='starrocks')}")

    print("\nPhysical Properties (table_properties):")
    for key, value in model.physical_properties.items():
        print(f"  {key}:")
        print(f"    Type: {type(value).__name__}")
        if isinstance(value, exp.Expression):
            print(f"    SQL: {value.sql(dialect='starrocks')}")
        else:
            print(f"    Value: {value}")

    print("\nColumn Definitions:")
    for col_name, col_type in model.columns_to_types.items():
        print(f"  {col_name}: {col_type.sql(dialect='starrocks')}")

    print_section("STEP 2: Intercepting create_table() Parameters")

    # We'll intercept the base class method to see what parameters are passed
    captured_params = {}

    # Import the base adapter
    from sqlmesh.core.engine_adapter.base import EngineAdapter

    original_method = EngineAdapter._create_table_from_columns

    def capture_params(self, **kwargs):
        captured_params.update(kwargs)
        # Don't actually create the table
        return None

    with patch.object(EngineAdapter, '_create_table_from_columns', capture_params):
        try:
            # Get the adapter
            adapter = context.engine_adapter
            print(f"\nAdapter type: {type(adapter).__name__}")

            # Simulate creating the table (this would happen during plan application)
            table_name = model.fqn.replace('"', '')

            # Call the method that would create the table
            adapter._create_table_from_columns(
                table_name=table_name,
                target_columns_to_types=model.columns_to_types,
                partitioned_by=model.partitioned_by_,
                clustered_by=model.clustered_by,
                table_properties=model.physical_properties,
                table_description=model.description,
                storage_format=model.storage_format,
            )

            print("✓ Parameters captured successfully!")

        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()

    print_section("STEP 3: Captured Parameters")

    for param_name in sorted(captured_params.keys()):
        param_value = captured_params[param_name]
        print(f"\n{param_name}:")
        print(format_value(param_value, indent=1))

    print_section("STEP 4: Building Table Properties Expression")

    # Now let's see what the adapter would build
    try:
        adapter = context.engine_adapter

        # Build the properties expression
        properties_exp = adapter._build_table_properties_exp(
            partitioned_by=captured_params.get('partitioned_by'),
            clustered_by=captured_params.get('clustered_by'),
            table_properties=captured_params.get('table_properties'),
            target_columns_to_types=captured_params.get('target_columns_to_types'),
            table_description=captured_params.get('table_description'),
            storage_format=captured_params.get('storage_format'),
        )

        if properties_exp:
            print("\n✓ Properties Expression Built:")
            print(f"  Type: {type(properties_exp).__name__}")
            print(f"\n  Properties ({len(properties_exp.expressions)} items):")
            for i, prop in enumerate(properties_exp.expressions):
                print(f"    [{i}] {type(prop).__name__}")
                try:
                    sql = prop.sql(dialect='starrocks')
                    print(f"        SQL: {sql}")
                except Exception as e:
                    print(f"        (Could not generate SQL: {e})")

            print(f"\n  Full SQL:")
            try:
                full_sql = properties_exp.sql(dialect='starrocks', pretty=True)
                print(f"    {full_sql}")
            except Exception as e:
                print(f"    ✗ Error: {e}")
        else:
            print("\n✗ No properties expression was built")

    except Exception as e:
        print(f"\n✗ Error building properties: {e}")
        import traceback
        traceback.print_exc()

    print_section("STEP 5: Complete CREATE TABLE Statement")

    try:
        # Build a complete CREATE TABLE statement
        from sqlglot import parse_one

        # Build schema with columns
        columns = [
            exp.ColumnDef(
                this=exp.to_identifier(col_name),
                kind=col_type,
            )
            for col_name, col_type in model.columns_to_types.items()
        ]

        schema = exp.Schema(
            this=exp.to_table(table_name),
            expressions=columns
        )

        create_stmt = exp.Create(
            this=schema,
            kind="TABLE",
            exists=True,
            properties=properties_exp if properties_exp else None
        )

        print("\nGenerated SQL:")
        print("-" * 80)
        sql = create_stmt.sql(dialect='starrocks', pretty=True)
        print(sql)
        print("-" * 80)

    except Exception as e:
        print(f"\n✗ Error generating CREATE TABLE: {e}")
        import traceback
        traceback.print_exc()

    print_section("Summary")

    print("""
The flow is:

1. MODEL(...) definition in SQL
   ↓
2. SQLMesh parses into Python objects (exp.Expression trees)
   - partitioned_by_ → List[exp.Column]
   - clustered_by → List[exp.Column]
   - physical_properties → Dict[str, exp.Expression]
   ↓
3. Parameters passed to adapter._create_table_from_columns()
   - table_name: str
   - target_columns_to_types: Dict[str, exp.DataType]
   - partitioned_by: List[exp.Expression]
   - clustered_by: List[exp.Expression]
   - table_properties: Dict[str, Any]
   - table_description: str
   - storage_format: str
   ↓
4. Adapter builds properties expression
   - _build_table_properties_exp() creates exp.Properties
   - Contains exp.PartitionedByProperty, exp.DistributedByProperty, etc.
   ↓
5. Generator converts to SQL
   - StarRocks.Generator.sql(create_exp)
   - Final CREATE TABLE statement

Key insights:
- physical_properties in MODEL becomes table_properties parameter
- Use flattened properties like distributed_by_kind, distributed_by_columns
- SQLGlot Expression objects are used throughout for type safety
""")


if __name__ == "__main__":
    main()
