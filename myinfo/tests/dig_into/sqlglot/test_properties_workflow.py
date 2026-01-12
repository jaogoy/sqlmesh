"""
Comprehensive test to demonstrate with_properties and properties() function behavior
in SQLGlot for StarRocks dialect.

This test specifically focuses on:
1. How properties marked as POST_WITH are placed in PROPERTIES (...)
2. The relationship between with_properties() and properties() functions
3. The complete property placement workflow
"""

import sys
sys.path.insert(0, '/Users/lijiao/resources/git-open-source/etl/sqlglot')

from sqlglot import exp, parse_one
from sqlglot.dialects.starrocks import StarRocks

def print_section(title):
    """Helper to print section headers"""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

def test_property_placement_workflow():
    """
    Demonstrates the complete workflow of how properties are categorized and placed.
    """
    print_section("DEMONSTRATION: with_properties() and properties() Function Workflow")

    generator = StarRocks.Generator()

    # Create a CREATE TABLE with mixed properties
    sql = """
    CREATE TABLE demo (
        id INT,
        name STRING
    )
    ENGINE = OLAP
    PRIMARY KEY (id)
    DISTRIBUTED BY HASH(id) BUCKETS 8
    PROPERTIES (
        "replication_num" = "3",
        "in_memory" = "false",
        "storage_medium" = "SSD"
    )
    """

    print("\nInput SQL:")
    print(sql)

    # Parse the statement
    stmt = parse_one(sql, dialect='starrocks')

    print("\n" + "-" * 80)
    print("STEP 1: Property Classification")
    print("-" * 80)

    if isinstance(stmt, exp.Create):
        all_props = stmt.args.get('properties')
        if all_props:
            print(f"\nTotal properties in AST: {len(all_props.expressions)}")

            # Classify properties by location
            post_schema_props = []
            post_with_props = []

            for prop in all_props.expressions:
                loc = generator.PROPERTIES_LOCATION.get(prop.__class__)
                prop_name = prop.__class__.__name__

                if loc == exp.Properties.Location.POST_SCHEMA:
                    post_schema_props.append(prop)
                    print(f"  ├─ {prop_name:30} → POST_SCHEMA (root properties)")
                elif loc == exp.Properties.Location.POST_WITH:
                    post_with_props.append(prop)
                    print(f"  ├─ {prop_name:30} → POST_WITH (with properties)")

            print("\n" + "-" * 80)
            print("STEP 2: Creating Separate Property Collections")
            print("-" * 80)

            # Create separate Properties objects
            root_props_ast = exp.Properties(expressions=post_schema_props)
            with_props_ast = exp.Properties(expressions=post_with_props)

            print(f"\nPOST_SCHEMA properties collection: {len(root_props_ast.expressions)} items")
            for prop in root_props_ast.expressions:
                print(f"  - {prop.__class__.__name__}")

            print(f"\nPOST_WITH properties collection: {len(with_props_ast.expressions)} items")
            for prop in with_props_ast.expressions:
                print(f"  - {prop.__class__.__name__}")

            print("\n" + "-" * 80)
            print("STEP 3: Calling root_properties() Method")
            print("-" * 80)

            root_sql = generator.root_properties(root_props_ast)
            print(f"\nroot_properties() output:")
            print(f"  '{root_sql}'")
            print("\nThis generates properties separated by spaces (not wrapped in PROPERTIES)")

            print("\n" + "-" * 80)
            print("STEP 4: Calling with_properties() Method")
            print("-" * 80)

            with_sql = generator.with_properties(with_props_ast)
            print(f"\nwith_properties() output:")
            print(f"  '{with_sql}'")
            print("\nThis internally calls:")
            print(f"  properties(props, prefix='{generator.WITH_PROPERTIES_PREFIX}')")

            print("\n" + "-" * 80)
            print("STEP 5: Understanding properties() Method Parameters")
            print("-" * 80)

            print("\nThe properties() method signature:")
            print("  def properties(self, properties, prefix='', sep=', ', suffix='', wrapped=True)")
            print("\nDifferent parameter combinations:")

            # Test different combinations
            test_props = exp.Properties(expressions=[
                exp.Property(this=exp.Literal.string("key1"), value=exp.Literal.string("val1")),
                exp.Property(this=exp.Literal.string("key2"), value=exp.Literal.string("val2")),
            ])

            r1 = generator.properties(test_props, prefix="PROPERTIES", wrapped=True)
            print(f"\n  1. prefix='PROPERTIES', wrapped=True:")
            print(f"     → {r1}")

            r2 = generator.properties(test_props, prefix="WITH", wrapped=True)
            print(f"\n  2. prefix='WITH', wrapped=True:")
            print(f"     → {r2}")

            r3 = generator.properties(test_props, prefix="", wrapped=True)
            print(f"\n  3. prefix='', wrapped=True:")
            print(f"     → {r3}")

            r4 = generator.properties(test_props, prefix="", wrapped=False)
            print(f"\n  4. prefix='', wrapped=False:")
            print(f"     → {r4}")

            r5 = generator.properties(test_props, prefix="PROPERTIES", sep=" | ", wrapped=True)
            print(f"\n  5. prefix='PROPERTIES', sep=' | ', wrapped=True:")
            print(f"     → {r5}")

            print("\n" + "-" * 80)
            print("STEP 6: Final SQL Generation")
            print("-" * 80)

            final_sql = stmt.sql(dialect='starrocks', pretty=True)
            print(f"\n{final_sql}")

            print("\nKey Observations:")
            print("  1. POST_SCHEMA properties appear immediately after the schema definition")
            print("  2. POST_WITH properties are wrapped in PROPERTIES (...)")
            print("  3. with_properties() uses WITH_PROPERTIES_PREFIX (which is 'PROPERTIES' for StarRocks)")
            print("  4. properties() can wrap expressions in parentheses when wrapped=True")

def test_property_types_classification():
    """
    Show which property types go to POST_WITH vs POST_SCHEMA
    """
    print_section("Property Type Classification in StarRocks")

    generator = StarRocks.Generator()

    print("\nProperties that go to POST_SCHEMA (appear before PROPERTIES clause):")
    print("-" * 80)
    for prop_class, location in sorted(generator.PROPERTIES_LOCATION.items(), key=lambda x: x[0].__name__):
        if location == exp.Properties.Location.POST_SCHEMA:
            print(f"  • {prop_class.__name__}")

    print("\nProperties that go to POST_WITH (appear in PROPERTIES clause):")
    print("-" * 80)
    for prop_class, location in sorted(generator.PROPERTIES_LOCATION.items(), key=lambda x: x[0].__name__):
        if location == exp.Properties.Location.POST_WITH:
            print(f"  • {prop_class.__name__}")

def test_with_properties_prefix():
    """
    Show how WITH_PROPERTIES_PREFIX affects the output
    """
    print_section("WITH_PROPERTIES_PREFIX Configuration")

    generator = StarRocks.Generator()

    print(f"\nStarRocks WITH_PROPERTIES_PREFIX: '{generator.WITH_PROPERTIES_PREFIX}'")
    print("\nThis means all POST_WITH properties will be prefixed with: PROPERTIES")

    test_props = exp.Properties(expressions=[
        exp.Property(this=exp.Literal.string("test_key"), value=exp.Literal.string("test_value")),
    ])

    result = generator.with_properties(test_props)
    print(f"\nExample output from with_properties():")
    print(f"  {result}")

    print("\nNote: The prefix 'PROPERTIES' is automatically added by with_properties() function")

if __name__ == "__main__":
    test_property_placement_workflow()
    test_property_types_classification()
    test_with_properties_prefix()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Key Findings:
1. Properties marked as POST_WITH ARE placed in the PROPERTIES (...) clause
2. with_properties() calls properties() with prefix=WITH_PROPERTIES_PREFIX
3. For StarRocks, WITH_PROPERTIES_PREFIX = 'PROPERTIES'
4. The properties() function handles formatting and wrapping
5. POST_SCHEMA properties appear before the PROPERTIES clause (not wrapped)
6. POST_WITH properties appear inside PROPERTIES (...) clause (wrapped)

The workflow is:
  parse() → classify by PROPERTIES_LOCATION → split into collections →
  root_properties() for POST_SCHEMA + with_properties() for POST_WITH →
  combine into final SQL
    """)
    print("=" * 80)
