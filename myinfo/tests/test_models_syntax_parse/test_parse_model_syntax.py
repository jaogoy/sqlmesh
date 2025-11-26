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

# Add paths
base_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(base_dir))

# Import shared test utilities
from myinfo.tests.test_utils import (
    analyze_expression,
    print_structure
)

from myinfo.tests.test_init_models import (
    create_context,
    list_models,
    get_model_by_name,
    load_single_model
)

try:
    from sqlmesh.core.model import Model
    from sqlglot import exp, parse_one
except ImportError as e:
    print(f"Import error: {e}")
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
        help='Model short name to test: range, list, expr, simple'
    )
    return parser.parse_args()


def analyze_physical_partition_properties(props: dict, model_dialect: str):
    """
    Analyze partition-related properties in physical_properties

    Args:
        props: physical_properties dictionary
        model_dialect: Model dialect for parsing
    """
    partition_keys = ['partition_by', 'partitioned_by', 'partitions']

    found_any = False
    for key in partition_keys:
        if key not in props:
            continue

        found_any = True
        value = props[key]

        print(f"\n{'-' * 80}")
        print(f"Property: {key}")
        print(f"Type: {type(value).__name__}")
        print(f"Raw value: {value}")

        # Analyze the expression structure
        if isinstance(value, exp.Expression):
            print(f"\nExpression Analysis:")
            structure = analyze_expression(value)
            print_structure(structure)

            # Try to get SQL representation
            if hasattr(value, 'sql'):
                print(f"\nSQL: {value.sql(dialect=model_dialect)}")

        elif isinstance(value, list):
            print(f"\nList with {len(value)} items:")
            for i, item in enumerate(value):
                print(f"\n  [{i}] Type: {type(item).__name__}")
                if isinstance(item, exp.Expression):
                    structure = analyze_expression(item)
                    print(f"  [{i}] Structure:")
                    print_structure(structure, indent=6)
                    if hasattr(item, 'sql'):
                        print(f"  [{i}] SQL: {item.sql(dialect=model_dialect)}")
                else:
                    print(f"  [{i}] Value: {item}")

        elif isinstance(value, str):
            # Try to parse the string as SQL expression
            print(f"\nString value: '{value}'")
            print(f"Attempting to parse as expression...")
            try:
                parsed = parse_one(value, dialect=model_dialect)
                print(f"Parsed type: {type(parsed).__name__}")
                structure = analyze_expression(parsed)
                print(f"\nParsed Structure:")
                print_structure(structure)
            except Exception as e:
                print(f"Cannot parse as expression: {e}")

        else:
            print(f"\nUnhandled type: {type(value)}")

    if not found_any:
        print("\n  (No partition-related properties found)")

    return found_any


def analyze_physical_distribution_properties(props: dict, model_dialect: str):
    """
    Analyze distribution-related properties in physical_properties

    Args:
        props: physical_properties dictionary
        model_dialect: Model dialect for parsing
    """
    distribution_key = 'distributed_by'

    if distribution_key not in props:
        print("\n  (No distributed_by property found)")
        return False

    value = props[distribution_key]

    print(f"\n{'-' * 80}")
    print(f"Property: {distribution_key}")
    print(f"Type: {type(value).__name__}")
    print(f"Raw value: {value}")

    # Analyze the expression structure
    if isinstance(value, exp.Expression):
        print(f"\nExpression Analysis:")
        structure = analyze_expression(value)
        print_structure(structure)

        # Try to get SQL representation
        if hasattr(value, 'sql'):
            print(f"\nSQL: {value.sql(dialect=model_dialect)}")

    elif isinstance(value, str):
        print(f"\nString value: '{value}'")
        print(f"Attempting to parse as expression...")
        try:
            parsed = parse_one(value, dialect=model_dialect)
            print(f"Parsed type: {type(parsed).__name__}")
            structure = analyze_expression(parsed)
            print(f"\nParsed Structure:")
            print_structure(structure)
        except Exception as e:
            print(f"Cannot parse as expression: {e}")

    else:
        print(f"\nUnhandled type: {type(value)}")

    return True


def analyze_physical_ordering_properties(props: dict, model_dialect: str):
    """
    Analyze ordering-related properties in physical_properties

    Args:
        props: physical_properties dictionary
        model_dialect: Model dialect for parsing
    """
    ordering_key = 'order_by'

    if ordering_key not in props:
        print("\n  (No order_by property found)")
        return False

    value = props[ordering_key]

    print(f"\n{'-' * 80}")
    print(f"Property: {ordering_key}")
    print(f"Type: {type(value).__name__}")
    print(f"Raw value: {value}")

    # Analyze the expression structure
    if isinstance(value, exp.Expression):
        print(f"\nExpression Analysis:")
        structure = analyze_expression(value)
        print_structure(structure)

        # Try to get SQL representation
        if hasattr(value, 'sql'):
            print(f"\nSQL: {value.sql(dialect=model_dialect)}")

    elif isinstance(value, str):
        print(f"\nString value: '{value}'")
        print(f"Attempting to parse as expression...")
        try:
            parsed = parse_one(value, dialect=model_dialect)
            print(f"Parsed type: {type(parsed).__name__}")
            structure = analyze_expression(parsed)
            print(f"\nParsed Structure:")
            print_structure(structure)
        except Exception as e:
            print(f"Cannot parse as expression: {e}")

    else:
        print(f"\nUnhandled type: {type(value)}")

    return True


def analyze_physical_key_properties(props: dict, model_dialect: str):
    """
    Analyze table key properties in physical_properties

    Args:
        props: physical_properties dictionary
        model_dialect: Model dialect for parsing
    """
    key_types = ['primary_key', 'duplicate_key', 'unique_key', 'aggregate_key']

    found_any = False
    for key in key_types:
        if key not in props:
            continue

        found_any = True
        value = props[key]

        print(f"\n{'-' * 80}")
        print(f"Property: {key}")
        print(f"Type: {type(value).__name__}")
        print(f"Raw value: {value}")

        # Analyze the expression structure
        if isinstance(value, exp.Expression):
            print(f"\nExpression Analysis:")
            structure = analyze_expression(value)
            print_structure(structure)

            # Try to get SQL representation
            if hasattr(value, 'sql'):
                print(f"\nSQL: {value.sql(dialect=model_dialect)}")

        elif isinstance(value, str):
            print(f"\nString value: '{value}'")

        else:
            print(f"\nUnhandled type: {type(value)}")

    if not found_any:
        print("\n  (No key properties found)")

    return found_any
MODEL_MAP = {
    # Basic partition tests
    'simple': 'test_simple_partition',
    'range': 'test_range_partition',
    'list': 'test_list_partition',
    'expr': 'test_expression_partition',
    'all_partition': 'test_all_partitions',
    'part': 'test_all_partitions',

    # Multi-column partition tests
    'multi': 'test_range_multi_columns',
    'range_multi': 'test_range_multi_columns',
    'list_multi': 'test_list_multi_columns',

    # Distribution tests
    'all_distribution': 'test_distribution_hash',
    'dist': 'test_distribution_hash',
    'hash': 'test_distribution_hash',
    'random': 'test_distribution_random',

    # Ordering tests
    'order': 'test_order_by',

    # Table key tests
    'keys': 'test_table_keys',
    'duplicate': 'test_table_keys',

    # Comprehensive test
    'comp': 'test_comprehensive_properties',
    'comprehensive': 'test_comprehensive_properties',
}


def main():
    args = parse_args()

    print("=" * 80)
    print("  SQLMesh Model Property Parsing Test")
    print("=" * 80)

    # Get test directory
    test_dir = Path(__file__).parent

    # Get the model to analyze
    if args.model:
        # Map short name to model name
        search_name = MODEL_MAP.get(args.model, args.model)
        print(f"\nTarget model: {args.model} -> {search_name}")
    else:
        search_name = 'test_range_partition'
        print(f"\nTarget model: {search_name} (default)")
        print(f"  Tip: Use -m range, list, expr, or simple")

    # Quick load: Create context and get model
    print("\nLoading model...")
    result = load_single_model(test_dir, search_name, verbose=True)

    if not result:
        print(f"\n✗ Model not found: {search_name}")
        # Show available models
        context = create_context(test_dir, verbose=False)
        if context:
            print(f"\nAvailable models:")
            for name in sorted(context.models.keys()):
                short = name.split('.')[-1].strip('"')
                print(f"  - {short}")
        return

    full_name, model = result
    short_name = full_name.split('.')[-1].strip('"')

    print(f"\n{'=' * 80}")
    print(f"Model: {short_name}")
    print('=' * 80)
    print(f"  Full name: {full_name}")
    print(f"  Type: {type(model).__name__}")
    print(f"  Kind: {model.kind}")
    print(f"  Dialect: {model.dialect}")

    # Print all model attributes
    print("\n" + "=" * 80)
    print("Basic Model Attributes")
    print("=" * 80)

    attrs_to_check = [
        'name', 'dialect', 'owner', 'kind', 'cron', 'start',
        'partitioned_by_', 'clustered_by', 'storage_format', 'table_format',
        'columns_to_types', 'annotated_columns'
    ]

    for attr in attrs_to_check:
        if hasattr(model, attr):
            value = getattr(model, attr)
            if value is None or value == '' or value == []:
                continue

            print(f"\n{attr}:")
            if attr == 'columns_to_types' and value:
                for col_name, col_type in value.items():
                    print(f"  - {col_name}: {col_type}")
            else:
                print(f"  {value}")

    # Partition analysis - directly print the whole structure
    if hasattr(model, 'partitioned_by_') and model.partitioned_by_:
        print("\n" + "=" * 80)
        print("PARTITIONED_BY STRUCTURE ANALYSIS")
        print("=" * 80)

        partitioned_by = model.partitioned_by_
        print(f"\nType: {type(partitioned_by).__name__}")
        print(f"Length: {len(partitioned_by)}")
        print(f"\nFull Structure:")
        print("-" * 80)

        # partitioned_by is a list, analyze each expression in it
        if len(partitioned_by) == 1:
            # Single partition - show the expression directly
            structure = analyze_expression(partitioned_by[0])
        else:
            # Multiple partitions - show as a list of expressions
            structure = [analyze_expression(expr) for expr in partitioned_by]

        print_structure(structure)

        # Also print simple representation
        print(f"\nSimple Representation:")
        print("-" * 80)
        for i, expr in enumerate(partitioned_by):
            if hasattr(expr, 'sql'):
                print(f"  [{i}] {type(expr).__name__}: {expr.sql()}")
            else:
                print(f"  [{i}] {type(expr).__name__}: {expr}")
    else:
        print("\n  (No partitioned_by defined)")

    # Check for physical_properties / table_properties
    print("\n" + "=" * 80)
    print("Physical Properties")
    print("=" * 80)

    # Try different attribute names
    found_props = False
    physical_props = None

    for attr_name in ['physical_properties', 'table_properties', '_table_properties']:
        if hasattr(model, attr_name):
            value = getattr(model, attr_name)
            if value:
                found_props = True
                physical_props = value
                print(f"\n{attr_name}:")
                if isinstance(value, dict):
                    for k, v in value.items():
                        print(f"  {k}: {v} (type: {type(v).__name__})")
                else:
                    print(f"  {value} (type: {type(value).__name__})")

    if not found_props:
        print("\n  (No physical properties defined)")

    # Detailed analysis of partition-related properties
    if physical_props and isinstance(physical_props, dict):
        print("\n" + "=" * 80)
        print("PARTITION PROPERTIES IN physical_properties")
        print("=" * 80)

        analyze_physical_partition_properties(physical_props, model.dialect)

        print("\n" + "=" * 80)
        print("DISTRIBUTION PROPERTIES IN physical_properties")
        print("=" * 80)
        analyze_physical_distribution_properties(physical_props, model.dialect)

        print("\n" + "=" * 80)
        print("ORDERING PROPERTIES IN physical_properties")
        print("=" * 80)
        analyze_physical_ordering_properties(physical_props, model.dialect)

        print("\n" + "=" * 80)
        print("TABLE KEY PROPERTIES IN physical_properties")
        print("=" * 80)
        analyze_physical_key_properties(physical_props, model.dialect)


if __name__ == "__main__":
    main()
