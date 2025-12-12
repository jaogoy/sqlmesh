# Test Real Model Value Parsing Behavior
#
# This test ACTUALLY parses MODEL definitions using SQLMesh's parser
# and examines what types are present in physical_properties.
#
# This is the ONLY way to understand real parsing behavior.

import pytest
from sqlglot import exp
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core import dialect as d


class TestRealModelValueParsing:
    """
    Test how SQLMesh ACTUALLY parses different value forms in MODEL definitions.

    This uses load_sql_based_model to parse real MODEL syntax and examines
    the resulting physical_properties dictionary to see what expression types
    SQLMesh produces.
    """

    @pytest.mark.parametrize(
        "property_syntax,property_name,expected_type,type_check,description",
        [
            # ==================================================================
            # Category 1: String Literals (Quoted)
            # IMPORTANT: SQLMesh parser converts quoted strings to Column!
            # ==================================================================
            (
                'primary_key = "id, dt"',
                "primary_key",
                exp.Column,
                lambda v: v.name == "id, dt" and v.this.quoted,
                'String literal: "id, dt" → Column(quoted=True)',
            ),
            (
                'order_by = "(id, timestamp)"',
                "order_by",
                exp.Column,
                lambda v: v.name == "(id, timestamp)" and v.this.quoted,
                'String with parens: "(id, timestamp)" → Column(quoted=True)',
            ),
            (
                'order_by = "id"',
                "order_by",
                exp.Column,
                lambda v: v.name == "id" and v.this.quoted,
                'Single string: "id" → Column(quoted=True)',
            ),
            # single quote
            (
                "primary_key = 'id, dt'",
                "primary_key",
                exp.Literal,
                lambda v: v.this == "id, dt",
                'String literal: "id, dt" → Column(quoted=True)',
            ),
            (
                "order_by = '(id, timestamp)'",
                "order_by",
                exp.Literal,
                lambda v: v.this == "(id, timestamp)",
                'String with parens: "(id, timestamp)" → Column(quoted=True)',
            ),
            (
                "order_by = 'id'",
                "order_by",
                exp.Literal,
                lambda v: v.this == "id",
                'Single string: "id" → Column(quoted=True)',
            ),
            # ==================================================================
            # Category 2: Tuple Expressions
            # ==================================================================
            (
                "primary_key = (id, dt)",
                "primary_key",
                exp.Tuple,
                lambda v: (
                    len(v.expressions) == 2
                    and all(isinstance(e, exp.Column) for e in v.expressions)
                ),
                "Tuple: (id, dt) → Tuple with Column nodes",
            ),
            (
                "primary_key = (id)",
                "primary_key",
                exp.Paren,
                lambda v: isinstance(v.this, exp.Column),
                "Single paren: (id) → Paren with Column",
            ),
            (
                "primary_key = id",
                "primary_key",
                exp.Column,
                lambda v: v.name == "id",
                "Bare identifier: id → Column",
            ),
            # ==================================================================
            # Category 3: Structured Tuples (key=value)
            # ==================================================================
            (
                "distributed_by = (kind='HASH', columns=(id, dt), buckets=10)",
                "distributed_by",
                exp.Tuple,
                lambda v: (
                    len(v.expressions) == 3
                    and all(isinstance(e, exp.EQ) for e in v.expressions)
                ),
                "Structured: (kind='HASH', ...) → Tuple with EQ nodes",
            ),
            (
                "distributed_by = (kind=RANDOM)",
                "distributed_by",
                exp.Paren,
                lambda v: isinstance(v.this, exp.EQ),
                "Single key=value: (kind='RANDOM') → Paren with EQ",
            ),
            # ==================================================================
            # Category 4: Numeric Literals
            # ==================================================================
            (
                "replication_num = 3",
                "replication_num",
                exp.Literal,
                lambda v: not v.is_string and v.this == "3",
                "Bare number: 3 → Literal (NOT string)",
            ),
            (
                "storage_cooldown_ttl = 86400",
                "storage_cooldown_ttl",
                exp.Literal,
                lambda v: not v.is_string and v.this == "86400",
                "Large number: 86400 → Literal (NOT string)",
            ),
            # ==================================================================
            # Category 5: String Numeric
            # IMPORTANT: Quoted numbers also become Column!
            # ==================================================================
            (
                'replication_num = "2"',
                "replication_num",
                exp.Column,
                lambda v: v.name == "2" and v.this.quoted,
                'Quoted number: "2" → Column(quoted=True)',
            ),
            # ==================================================================
            # Category 6: Boolean-like Strings
            # IMPORTANT: All quoted strings become Column!
            # ==================================================================
            (
                'enable_persistent_index = "true"',
                "enable_persistent_index",
                exp.Column,
                lambda v: v.name.lower() == "true" and v.this.quoted,
                'Boolean string: "true" → Column(quoted=True)',
            ),
            (
                'in_memory = "FALSE"',
                "in_memory",
                exp.Column,
                lambda v: v.name == "FALSE" and v.this.quoted,
                'Boolean string: "FALSE" → Column(quoted=True)',
            ),
            # ==================================================================
            # NOTE: Complex function expressions are not supported in MODEL syntax
            # SQLMesh physical_properties only supports simple values
            # ==================================================================
        ],
    )
    def test_real_parsing_behavior(
        self,
        property_syntax: str,
        property_name: str,
        expected_type: type,
        type_check,
        description: str,
    ):
        """
        Parse a real MODEL definition and verify the type in physical_properties.

        This is the CORRECT way to test parsing - use SQLMesh's actual parser
        and examine the results.
        """
        # Build a complete MODEL definition
        model_definition = f"""
        MODEL (
            name test_db.test_model,
            kind FULL,
            physical_properties (
                {property_syntax}
            )
        );

        SELECT 1 as id, '2024-01-01' as dt, NOW() as ts, 'US' as region;
        """

        # Parse using SQLMesh
        parsed_statements = d.parse(model_definition, default_dialect="")
        model = load_sql_based_model(
            parsed_statements,
            defaults={"dialect": ""},
        )

        # Get physical_properties
        physical_props = model.physical_properties

        # Verify property exists
        assert property_name in physical_props, (
            f"Property '{property_name}' not found in physical_properties.\n"
            f"Available properties: {list(physical_props.keys())}"
        )

        # Get the actual parsed value
        actual_value = physical_props[property_name]

        # Check type
        assert isinstance(actual_value, expected_type), (
            f"\n{description}\n"
            f"Expected type: {expected_type.__name__}\n"
            f"Actual type: {type(actual_value).__name__}\n"
            f"Actual value: {actual_value}\n"
            f"Value repr: {repr(actual_value)}"
        )

        # Check detailed type properties
        assert type_check(actual_value), (
            f"\n{description}\n"
            f"Type check failed for: {actual_value}\n"
            f"Type: {type(actual_value).__name__}"
        )

        # Print for documentation
        print(f"✓ {description}")
        print(f"  Syntax: {property_syntax}")
        print(f"  Type: {type(actual_value).__name__}")
        if isinstance(actual_value, exp.Literal):
            print(f"  is_string: {actual_value.is_string}")
            print(f"  value: {actual_value.this}")
        print()

    def test_parsing_reference_table(self):
        """
        Generate a comprehensive reference table by parsing multiple properties.

        This shows ALL the parsing behaviors in one test.
        """
        # Build a model with MANY different property forms
        model_definition = """
        MODEL (
            name test_db.comprehensive_model,
            kind FULL,
            physical_properties (
                -- String literals
                prop_str_comma = "id, dt",
                prop_str_paren = "(id, dt)",
                prop_str_single = "id",

                -- Tuples
                prop_tuple_two = (col1, col2),
                prop_tuple_one = (col1),
                prop_bare = col1,

                -- Structured
                prop_struct_full = (kind='HASH', columns=(id, dt), buckets=10),
                prop_struct_simple = (kind='RANDOM'),

                -- Numeric
                prop_num_bare = 3,
                prop_num_large = 86400,
                prop_num_quoted = "2",

                -- Boolean-like
                prop_bool_true = "true",
                prop_bool_false = "FALSE",

                -- Expressions
                prop_func_trunc = date_trunc('day', ts),
                prop_func_range = RANGE(dt),
                prop_mixed = (date_trunc('day', ts), region),

                -- Edge case
                prop_empty = ()
            )
        );

        SELECT
            1 as id,
            '2024-01-01' as dt,
            NOW() as ts,
            'US' as region,
            1 as col1,
            1 as col2;
        """

        # Parse
        parsed_statements = d.parse(model_definition, default_dialect="")
        model = load_sql_based_model(parsed_statements, defaults={"dialect": ""})
        props = model.physical_properties

        # Print comprehensive table
        print("\n" + "=" * 100)
        print("REAL MODEL VALUE PARSING - COMPREHENSIVE REFERENCE TABLE")
        print("=" * 100)
        print(f"{'Property Syntax':<50} | {'Parsed Type':<20} | {'Details':<25}")
        print("-" * 100)

        test_cases = [
            ("prop_str_comma", 'prop_str_comma = "id, dt"'),
            ("prop_str_paren", 'prop_str_paren = "(id, dt)"'),
            ("prop_str_single", 'prop_str_single = "id"'),
            ("prop_tuple_two", "prop_tuple_two = (col1, col2)"),
            ("prop_tuple_one", "prop_tuple_one = (col1)"),
            ("prop_bare", "prop_bare = col1"),
            ("prop_struct_full", "prop_struct_full = (kind='HASH', ...)"),
            ("prop_struct_simple", "prop_struct_simple = (kind='RANDOM')"),
            ("prop_num_bare", "prop_num_bare = 3"),
            ("prop_num_large", "prop_num_large = 86400"),
            ("prop_num_quoted", 'prop_num_quoted = "2"'),
            ("prop_bool_true", 'prop_bool_true = "true"'),
            ("prop_bool_false", 'prop_bool_false = "FALSE"'),
            ("prop_func_trunc", "prop_func_trunc = date_trunc('day', ts)"),
            ("prop_func_range", "prop_func_range = RANGE(dt)"),
            ("prop_mixed", "prop_mixed = (date_trunc(...), region)"),
            ("prop_empty", "prop_empty = ()"),
        ]

        for prop_name, syntax in test_cases:
            if prop_name in props:
                value = props[prop_name]
                type_name = type(value).__name__

                # Get details
                details = ""
                if isinstance(value, exp.Literal):
                    details = f"is_string={value.is_string}"
                elif isinstance(value, exp.Tuple):
                    details = f"{len(value.expressions)} items"
                elif isinstance(value, exp.Paren):
                    details = "single item in parens"
                elif isinstance(value, exp.Column):
                    details = f"name='{value.name}'"
                elif isinstance(value, exp.Anonymous):
                    details = f"func='{value.this}'"

                print(f"{syntax:<50} | {type_name:<20} | {details:<25}")

        print("=" * 100)
        print("\nCRITICAL FINDINGS (REAL PARSING BEHAVIOR):")
        print("  1. Quoted strings → exp.Column with quoted=True (NOT Literal!)")
        print("  2. Bare numbers → exp.Literal with is_string=False")
        print("  3. (a, b) with 2+ items → exp.Tuple")
        print("  4. (a) single item → exp.Paren")
        print("  5. Bare identifiers → exp.Column with quoted=False")
        print("  6. (key=value, ...) multiple → exp.Tuple with exp.EQ children")
        print("  7. (key=value) single → exp.Paren with exp.EQ child")
        print(
            "  8. Functions like date_trunc() → specific exp types (DateTrunc, Anonymous, etc.)"
        )
        print("=" * 100)
        print("\nIMPORTANT FOR STARROCKS ADAPTER:")
        print('  When model has: primary_key = "id, dt"')
        print("  physical_properties contains: exp.Column(name='id, dt', quoted=True)")
        print("  StarRocks PropertyValidator must handle:")
        print('    • exp.Column with quoted=True (from string literals like "id, dt")')
        print("    • exp.Tuple/Paren (from tuple syntax like (id, dt))")
        print("    • exp.Column with quoted=False (from bare identifiers like id)")
        print("=" * 100)


class TestEdgeCases:
    """Test edge cases in real parsing."""

    def test_single_vs_multiple_structured_values(self):
        """
        Verify difference between single and multiple key=value pairs.

        - Single pair: (kind='RANDOM') → Paren
        - Multiple pairs: (kind='HASH', buckets=10) → Tuple
        """
        model_def_single = """
        MODEL (
            name test_db.test_model,
            kind FULL,
            physical_properties (
                prop_single = (kind='RANDOM')
            )
        );
        SELECT 1 as id;
        """

        parsed = d.parse(model_def_single, default_dialect="")
        model = load_sql_based_model(parsed, defaults={"dialect": ""})
        value_single = model.physical_properties["prop_single"]

        assert isinstance(value_single, exp.Paren)
        assert isinstance(value_single.this, exp.EQ)
        print("✓ Single key=value: (kind='RANDOM') → Paren with EQ")

        model_def_multiple = """
        MODEL (
            name test_db.test_model2,
            kind FULL,
            physical_properties (
                prop_multiple = (kind='HASH', buckets=10)
            )
        );
        SELECT 1 as id;
        """

        parsed2 = d.parse(model_def_multiple, default_dialect="")
        model2 = load_sql_based_model(parsed2, defaults={"dialect": ""})
        value_multiple = model2.physical_properties["prop_multiple"]

        assert isinstance(value_multiple, exp.Tuple)
        assert len(value_multiple.expressions) == 2
        print("✓ Multiple key=value: (kind='HASH', buckets=10) → Tuple with EQ nodes")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
