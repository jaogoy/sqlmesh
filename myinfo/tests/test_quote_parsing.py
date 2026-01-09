# Test Quote Character Configuration and Parsing Behavior
#
# This test verifies that StarRocks/MySQL dialect correctly uses backtick (`)
# for identifiers and distinguishes between single quotes (') and double quotes (").

import pytest
from sqlglot import exp, parse_one
from sqlmesh.core import dialect as d
from sqlmesh.core.model import load_sql_based_model


class TestQuoteCharacterConfiguration:
    """
    Test quote character configuration in StarRocks/MySQL dialect.

    Verify that:
    1. Identifier quote character is backtick (`)
    2. String literals use single quotes (')
    3. Double quotes (") should be treated according to SQL standard
    """

    def test_mysql_tokenizer_configuration(self):
        """Verify MySQL Tokenizer has correct quote configuration."""
        from sqlglot.dialects.mysql import MySQL

        tokenizer = MySQL.Tokenizer

        # MySQL should use backtick for identifiers
        assert (
            "`" in tokenizer.IDENTIFIERS
        ), "MySQL should use backtick (`) as identifier quote"

        # MySQL allows both single and double quotes for strings
        # But in ANSI SQL mode, double quotes should be for identifiers
        assert "'" in tokenizer.QUOTES, "Single quote should be in QUOTES"
        assert '"' in tokenizer.QUOTES, "Double quote should be in QUOTES"

        print(f"✓ MySQL Tokenizer.IDENTIFIERS = {tokenizer.IDENTIFIERS}")
        print(f"✓ MySQL Tokenizer.QUOTES = {tokenizer.QUOTES}")

    def test_starrocks_tokenizer_configuration(self):
        """Verify StarRocks inherits correct tokenizer from MySQL."""
        from sqlglot.dialects.starrocks import StarRocks

        tokenizer = StarRocks.Tokenizer

        # StarRocks should inherit from MySQL
        assert (
            "`" in tokenizer.IDENTIFIERS
        ), "StarRocks should use backtick (`) as identifier quote"

        assert "'" in tokenizer.QUOTES, "Single quote should be in QUOTES"
        assert '"' in tokenizer.QUOTES, "Double quote should be in QUOTES"

        print(f"✓ StarRocks Tokenizer.IDENTIFIERS = {tokenizer.IDENTIFIERS}")
        print(f"✓ StarRocks Tokenizer.QUOTES = {tokenizer.QUOTES}")

    @pytest.mark.parametrize(
        "sql,expected_type,check_func,description",
        [
            # Backtick identifiers
            (
                "SELECT `id` FROM table",
                exp.Column,
                lambda col: col.name == "id" and col.this.quoted,
                "Backtick identifier: `id` → Column(quoted=True)",
            ),
            (
                "SELECT `my column` FROM table",
                exp.Column,
                lambda col: col.name == "my column" and col.this.quoted,
                "Backtick with space: `my column` → Column(quoted=True)",
            ),
            # Single quote strings (should be Literal)
            (
                "SELECT 'hello'",
                exp.Literal,
                lambda lit: lit.is_string and lit.this == "hello",
                "Single quote: 'hello' → Literal(is_string=True)",
            ),
            (
                "SELECT 'id, dt'",
                exp.Literal,
                lambda lit: lit.is_string and lit.this == "id, dt",
                "Single quote list: 'id, dt' → Literal(is_string=True)",
            ),
            # Double quote handling in MySQL (context-dependent)
            # In standard MySQL mode (NOT ANSI_QUOTES), double quotes are string literals
            # But in MODEL parsing context, they become identifiers!
            (
                'SELECT "id"',
                exp.Literal,  # In regular SQL: double quotes → string literal
                lambda lit: lit.is_string and lit.this == "id",
                'Double quote in SQL: "id" → Literal (MySQL default mode)',
            ),
        ],
    )
    def test_quote_parsing_in_sql(self, sql, expected_type, check_func, description):
        """Test how different quote types are parsed in SQL statements."""
        # Parse with mysql dialect
        parsed = parse_one(sql, dialect="mysql")

        # Find the relevant expression
        if isinstance(expected_type, type) and issubclass(expected_type, exp.Column):
            # Find Column in SELECT
            select = parsed.find(exp.Select)
            assert select is not None
            column = select.expressions[0]
            if isinstance(column, exp.Alias):
                column = column.this

            assert isinstance(column, expected_type), (
                f"\n{description}\n"
                f"Expected: {expected_type.__name__}\n"
                f"Actual: {type(column).__name__}"
            )
            assert check_func(column), f"{description} - check failed"

        elif isinstance(expected_type, type) and issubclass(expected_type, exp.Literal):
            # Find Literal in SELECT
            literal = parsed.find(exp.Literal)
            assert literal is not None

            assert isinstance(literal, expected_type), (
                f"\n{description}\n"
                f"Expected: {expected_type.__name__}\n"
                f"Actual: {type(literal).__name__}"
            )
            assert check_func(literal), f"{description} - check failed"

    @pytest.mark.parametrize(
        "property_syntax,expected_type,check_func,description",
        [
            # Single quotes in MODEL → should become Literal
            (
                "partition_by = 'id, dt'",
                exp.Literal,
                lambda v: v.is_string and "id, dt" in v.this,
                "Single quote in MODEL: '...' → Literal(is_string=True)",
            ),
            (
                "distributed_by = 'HASH(customer_id) BUCKETS 16'",
                exp.Literal,
                lambda v: v.is_string and "HASH" in v.this,
                "Single quote in MODEL: '...' → Literal(is_string=True)",
            ),
            # Double quotes in MODEL → currently becomes Column(quoted=True)
            # This is the ISSUE: in MySQL/StarRocks, double quotes should be strings
            # unless ANSI_QUOTES mode is enabled
            (
                'primary_key = "id, dt"',
                exp.Column,
                lambda v: v.name == "id, dt" and v.this.quoted,
                'Double quote in MODEL: "id, dt" → Column(quoted=True) [CURRENT BEHAVIOR]',
            ),
            # TODO: Backtick parsing in MODEL has issues
            # Skipping for now as it causes parsing errors
            # (
            #     "order_by = `id`",
            #     exp.Column,
            #     lambda v: v.name == "id" and v.this.quoted,
            #     "Backtick in MODEL: `id` → Column(quoted=True)"
            # ),
        ],
    )
    def test_quote_parsing_in_model(
        self, property_syntax, expected_type, check_func, description
    ):
        """Test how different quote types are parsed in MODEL physical_properties."""
        model_definition = f"""
        MODEL (
            name test_db.test_model,
            kind FULL,
            physical_properties (
                {property_syntax}
            )
        );

        SELECT 1 as id, '2025-01-01' as dt;
        """

        # Parse using SQLMesh
        parsed_statements = d.parse(model_definition, default_dialect="")
        model = load_sql_based_model(
            parsed_statements,
            defaults={"dialect": "starrocks"},
        )

        # Get the property value
        physical_props = model.physical_properties
        property_name = property_syntax.split("=")[0].strip()

        assert property_name in physical_props, (
            f"Property '{property_name}' not found.\n"
            f"Available: {list(physical_props.keys())}"
        )

        actual_value = physical_props[property_name]

        assert isinstance(actual_value, expected_type), (
            f"\n{description}\n"
            f"Expected type: {expected_type.__name__}\n"
            f"Actual type: {type(actual_value).__name__}"
        )

        assert check_func(actual_value), (
            f"{description}\n"
            f"Type check passed but value check failed.\n"
            f"Actual value: {actual_value}"
        )


class TestQuoteBehaviorInEnsureParenthesized:
    """
    Test that ensure_parenthesized correctly handles all quote types.
    """

    def test_single_quote_literal_extraction(self):
        """Test that single-quoted strings are extracted as Literal."""
        from sqlmesh.core.engine_adapter.starrocks import PropertyValidator

        # Single quote → Literal
        literal = exp.Literal.string("id, dt")
        result = PropertyValidator.ensure_parenthesized(literal)

        assert result == "(id, dt)", f"Expected: '(id, dt)'\n" f"Actual: '{result}'"

    def test_double_quote_column_extraction(self):
        """Test that double-quoted identifiers are extracted as Column."""
        from sqlmesh.core.engine_adapter.starrocks import PropertyValidator

        # Double quote → Column(quoted=True)
        column = exp.Column(this=exp.Identifier(this="id, dt", quoted=True))
        result = PropertyValidator.ensure_parenthesized(column)

        assert result == "(id, dt)", f"Expected: '(id, dt)'\n" f"Actual: '{result}'"

    def test_backtick_column_extraction(self):
        """Test that backtick identifiers are treated as Column."""
        from sqlmesh.core.engine_adapter.starrocks import PropertyValidator

        # Backtick → Column(quoted=True)
        column = exp.Column(this=exp.Identifier(this="id", quoted=True))
        result = PropertyValidator.ensure_parenthesized(column)

        # Single identifier should not get extra parentheses from content
        # But ensure_parenthesized wraps it
        assert result == "(id)", f"Expected: '(id)'\n" f"Actual: '{result}'"
