"""
Shared utilities for SQLMesh syntax parsing tests

This module provides common functions for analyzing and printing
SQLGlot expression structures in a detailed way.
"""
from typing import Any, Dict, List, Union
from sqlglot.expressions import Expression


def analyze_expression(expr: Expression, depth: int = 0, max_depth: int = 5) -> Dict[str, Any]:
    """
    Recursively analyze expression structure

    Args:
        expr: SQLGlot Expression to analyze
        depth: Current recursion depth
        max_depth: Maximum recursion depth to prevent infinite loops

    Returns:
        Dictionary containing expression structure information
    """
    if depth > max_depth:
        return {"type": "MAX_DEPTH_REACHED", "value": str(expr)}

    result = {
        "type": type(expr).__name__,
        "class": str(expr.__class__),
        "sql": expr.sql() if hasattr(expr, 'sql') else str(expr),
        "key": expr.key if hasattr(expr, 'key') else None,
        "args": {}
    }

    # Analyze arguments
    if hasattr(expr, 'args') and expr.args:
        for key, value in expr.args.items():
            if isinstance(value, list):
                result["args"][key] = [
                    analyze_expression(item, depth + 1, max_depth) if hasattr(item, 'sql')
                    else {"type": type(item).__name__, "value": str(item)}
                    for item in value
                ]
            elif value is not None:
                if hasattr(value, 'sql'):
                    result["args"][key] = analyze_expression(value, depth + 1, max_depth)
                else:
                    result["args"][key] = {"type": type(value).__name__, "value": str(value)}

    return result


def print_structure(data: Union[Dict, List, Any], indent: int = 0, max_indent: int = 10) -> None:
    """
    Pretty print the structure dictionary or list

    Args:
        data: Data to print (dict, list, or other)
        indent: Current indentation level
        max_indent: Maximum indentation to prevent excessive nesting
    """
    if indent > max_indent:
        print("  " * indent + "... (max indent reached)")
        return

    prefix = "  " * indent

    if isinstance(data, dict):
        for key, value in data.items():
            if key in ['type', 'sql', 'key']:
                print(f"{prefix}{key}: {value}")
            elif key == 'args' and value:
                print(f"{prefix}{key}:")
                print_structure(value, indent + 1, max_indent)
            elif isinstance(value, (dict, list)):
                print(f"{prefix}{key}:")
                print_structure(value, indent + 1, max_indent)
            else:
                print(f"{prefix}{key}: {value}")
    elif isinstance(data, list):
        for i, item in enumerate(data):
            print(f"{prefix}[{i}]:")
            print_structure(item, indent + 1, max_indent)
    else:
        print(f"{prefix}{data}")


def print_expression_summary(expr: Expression, name: str = "Expression") -> None:
    """
    Print a summary of an expression

    Args:
        expr: SQLGlot Expression to summarize
        name: Name/label for this expression
    """
    print(f"\n{name}:")
    print(f"  Type: {type(expr).__name__}")
    if hasattr(expr, 'sql'):
        print(f"  SQL: {expr.sql()}")
    if hasattr(expr, 'key'):
        print(f"  Key: {expr.key}")
    if hasattr(expr, 'args') and expr.args:
        print(f"  Args keys: {list(expr.args.keys())}")


def print_expression_detailed(expr: Expression, name: str = "Expression", max_depth: int = 5) -> None:
    """
    Print detailed structure analysis of an expression

    Args:
        expr: SQLGlot Expression to analyze
        name: Name/label for this expression
        max_depth: Maximum recursion depth for structure analysis
    """
    print(f"\n{'=' * 80}")
    print(f"{name} - Detailed Structure Analysis")
    print('=' * 80)

    # Basic info
    print(f"Type: {type(expr).__name__}")
    print(f"SQL: {expr.sql() if hasattr(expr, 'sql') else str(expr)}")
    print(f"Key: {expr.key if hasattr(expr, 'key') else 'N/A'}")

    # Detailed structure
    print(f"\nStructure:")
    print('-' * 80)
    structure = analyze_expression(expr, max_depth=max_depth)
    print_structure(structure)

    # Key properties
    if hasattr(expr, 'args') and expr.args:
        print(f"\nKey Properties:")
        print('-' * 80)
        for key in ['this', 'expression', 'expressions', 'alias', 'name']:
            if key in expr.args and expr.args[key] is not None:
                value = expr.args[key]
                if isinstance(value, list):
                    print(f"  - {key}: [{len(value)} items]")
                    for i, item in enumerate(value[:3]):  # Show first 3
                        if hasattr(item, 'sql'):
                            print(f"      [{i}] {type(item).__name__}: {item.sql()}")
                        else:
                            print(f"      [{i}] {type(item).__name__}: {item}")
                elif hasattr(value, 'sql'):
                    print(f"  - {key}: {type(value).__name__} = {value.sql()}")
                else:
                    print(f"  - {key}: {type(value).__name__} = {value}")


def compare_expressions(expressions: List[tuple], title: str = "Expression Comparison") -> None:
    """
    Compare multiple expressions side by side

    Args:
        expressions: List of (name, expression) tuples
        title: Title for the comparison
    """
    print(f"\n{'=' * 80}")
    print(title)
    print('=' * 80)

    # Print summary table
    print(f"\n{'Name':<30} {'Type':<25} {'SQL':<25}")
    print('-' * 80)
    for name, expr in expressions:
        expr_type = type(expr).__name__
        expr_sql = expr.sql() if hasattr(expr, 'sql') else str(expr)
        # Truncate long SQL
        if len(expr_sql) > 22:
            expr_sql = expr_sql[:22] + "..."
        print(f"{name:<30} {expr_type:<25} {expr_sql:<25}")
