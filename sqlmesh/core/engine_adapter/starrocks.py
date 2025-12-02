from __future__ import annotations

import logging
import sqlglot
from sqlglot import exp
import typing as t

from sqlmesh.core.engine_adapter.base import (
    InsertOverwriteStrategy,
)
from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    set_catalog,
    to_schema,
)
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName

logger = logging.getLogger(__name__)


###############################################################################
# Declarative Type System for Property Validation and Normalization
###############################################################################
"""
Declarative type system for property validation and normalization.

This module provides a declarative way to define property types with clear separation
between validation (type checking) and normalization (type conversion).
"""
Validated = t.Any  # validated intermediate value (AST nodes, string, list...)
Normalized = t.Any  # final normalized output

# Allowed outputs for EnumType normalize / or general property outputs.
PROPERTY_OUTPUT_TYPES = {
    "str",           # "HASH"
    "identifier",    # exp.Identifier
    "literal",       # exp.Literal.string("HASH")
    "column",        # exp.Column(this="HASH")
    "ast_expr",      # generic exp.Expression
}


# ============================================================
# Fragment parser (robust-ish)
# ============================================================
def parse_fragment(text: str) -> t.Union[exp.Expression, t.List[exp.Expression]]:
    """
    Try to parse a DSL fragment into SQLGlot AST(s).

    Behavior:
    1. If parse_one succeeds, return the exp.Expression.
    2. If fails but text contains comma, split by commas and parse each part.
    3. If it's parenthesized like "(a, b)", parse and return exp.Tuple or list.
    4. If it's a simple token like "IDENT", return exp.Identifier.
    """
    if isinstance(text, exp.Expression):
        return text

    if not isinstance(text, str):
        raise TypeError("parse_fragment expects a string")

    s = text.strip()
    # 1) try full parse
    try:
        parsed = sqlglot.parse_one(s)
        return parsed
    except Exception:
        raise ValueError(f"Unable to parse fragment: {s}")


# ============================================================
# Base Type
# ============================================================
class DeclarativeType:
    """
    Base class for declarative type system.

    Design Philosophy:
    -----------------
    - validate(value): Type checking only - returns validated intermediate value or None
    - normalize(validated): Type conversion only - transforms to target output format

    Methods:
    --------
    validate(value) -> Optional[Validated]
        Check if value conforms to this type, maybe include some tiny different types
        Returns: Validated intermediate value if valid, None otherwise.

    normalize(validated) -> Normalized
        Convert validated intermediate value to final output format.
        Returns: Normalized value in target format.

    __call__(value) -> Normalized
        Convenience method: validate + normalize in one step.
    """

    def validate(self, value: t.Any) -> t.Optional[Validated]:
        """Check if value conforms to this type. Return validated value or None.
        String that can be parsed as literal
        """
        raise NotImplementedError(f"{self.__class__.__name__}.validate() must be implemented")

    def normalize(self, validated: Validated) -> Normalized:
        """Convert validated intermediate value to final output format."""
        # Default: identity transformation
        return validated

    def __call__(self, value: t.Any) -> Normalized:
        """Validate and normalize in one step."""
        validated = self.validate(value)
        if validated is None:
            raise ValueError(
                f"Value {value!r} does not conform to type {self.__class__.__name__}"
            )
        return self.normalize(validated)


# ============================================================
# Primitive Types
# ============================================================
class StringType(DeclarativeType):
    """
    String type validator.

    Accepts:
    - Python str only

    Validation: Returns the string if valid, None otherwise.
    Normalization: Returns the string as-is (identity).
    """

    def __init__(self, normalized_type: str = "str"):
        """
        Args:
            normalized_type: Target type for normalization.
                - "literal": Convert to exp.Literal.string()
                - "str": Keep as string (default)
                - "identifier": Convert to exp.Identifier
        """
        self.normalized_type = normalized_type

    def validate(self, value: t.Any) -> t.Optional[str]:
        """Check if value is a Python string. Returns string or None."""
        return value if isinstance(value, str) else None

    def normalize(self, validated: str) -> str:
        """Return string as-is (identity normalization)."""
        return validated


class LiteralType(DeclarativeType):
    """
    Literal type validator.

    Accepts:
    - exp.Literal only (from AST)
    - String that can be parsed as literal

    Validation: Returns exp.Literal if valid, None otherwise.
    Normalization: Converts to target type based on normalized_type parameter.
    """

    def __init__(self, normalized_type: t.Optional[str] = None):
        """
        Args:
            normalized_type: Target type for normalization.
                - None: Keep as exp.Literal (default)
                - "literal": Keep as exp.Literal
                - "str": Convert to Python string
        """
        self.normalized_type = normalized_type

    def validate(self, value: t.Any) -> t.Optional[exp.Literal]:
        """Check if value is a literal type. Returns exp.Literal or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Check if it's a Literal
        if isinstance(value, exp.Literal):
            return value

        return None

    def normalize(self, validated: exp.Literal) -> t.Union[exp.Literal, str]:
        """Convert to target type based on normalized_type."""
        if self.normalized_type == "str":
            return validated.this
        # None or "literal" - keep as-is
        return validated


class IdentifierType(DeclarativeType):
    """
    Identifier type validator.

    Accepts:
    - exp.Identifier only
    - String that can be parsed as identifier

    Validation: Returns exp.Identifier if valid, None otherwise.
    Normalization: Converts to target type based on normalized_type parameter.
    """

    def __init__(self, normalized_type: t.Optional[str] = None):
        """
        Args:
            normalized_type: Target type for normalization.
                - None: Keep as exp.Identifier (default)
                - "literal": Convert to exp.Literal.string()
                - "str": Convert to Python string
                - "identifier": Keep as exp.Identifier
                - "column": Convert to exp.Column
        """
        self.normalized_type = normalized_type

    def validate(self, value: t.Any) -> t.Optional[exp.Identifier]:
        """Check if value is an identifier type. Returns exp.Identifier or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Check if it's an Identifier
        if isinstance(value, exp.Identifier):
            return value

        return None

    def normalize(self, validated: exp.Identifier) -> t.Union[exp.Identifier, exp.Column, exp.Literal, str]:
        """Convert to target type based on normalized_type."""
        if self.normalized_type == "column":
            return exp.column(validated.this)
        if self.normalized_type == "literal":
            return exp.Literal.string(validated.this)
        if self.normalized_type == "str":
            return validated.this
        # None or "identifier" - keep as-is
        return validated


class ColumnType(DeclarativeType):
    """
    Column type validator.

    Accepts:
    - exp.Column only
    - String that can be parsed as column

    Validation: Returns exp.Column if valid, None otherwise.
    Normalization: Converts to target type based on normalized_type parameter.
    """

    def __init__(self, normalized_type: t.Optional[str] = None):
        """
        Args:
            normalized_type: Target type for normalization.
                - None: Keep as exp.Column (default)
                - "literal": Convert to exp.Literal.string()
                - "str": Convert to Python string
                - "identifier": Convert to exp.Identifier
                - "column": Keep as exp.Column
        """
        self.normalized_type = normalized_type

    def validate(self, value: t.Any) -> t.Optional[exp.Column]:
        """Check if value is a column type. Returns exp.Column or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Check if it's a Column
        if isinstance(value, exp.Column):
            return value

        return None

    def normalize(self, validated: exp.Column) -> t.Union[exp.Column, exp.Identifier, exp.Literal, str]:
        """Convert to target type based on normalized_type."""
        if self.normalized_type == "identifier":
            return exp.Identifier(this=validated.this)
        if self.normalized_type == "literal":
            return exp.Literal.string(validated.this)
        if self.normalized_type == "str":
            return validated.this
        # None or "column" - keep as-is
        return validated


class EqType(DeclarativeType):
    """
    EQ expression type validator (key=value pairs).

    Accepts:
    - exp.EQ(left, right)
    - String that can be parsed as key=value

    Validation: Returns (key_name, value_expr) tuple if valid, None otherwise.
    Normalization: Returns the (key, value) tuple as-is.
    """

    def validate(self, value: t.Any) -> t.Optional[t.Tuple[str, t.Any]]:
        """Check if value is an EQ expression. Returns (key, value) tuple or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Check if it's an EQ expression
        if isinstance(value, exp.EQ):
            # Extract key name from left side
            left = value.this
            # Extract value from right side
            right = value.expression

            key_name = None
            if isinstance(left, exp.Column):
                key_name = left.this.name if hasattr(left.this, 'name') else str(left.this)
            elif isinstance(left, exp.Identifier):
                key_name = left.this
            elif isinstance(left, str):
                key_name = left
            else:
                key_name = str(left)

            return (key_name, right)

        return None

    def normalize(self, validated: t.Tuple[str, t.Any]) -> t.Tuple[str, t.Any]:
        """Return (key, value) tuple as-is (identity normalization)."""
        return validated


class EnumType(DeclarativeType):
    """
    Enumerated value type validator.

    Accepts values from a predefined set of allowed values.

    Parameters:
    -----------
    valid_values : t.Sequence[str]
        List of allowed values (e.g., ["HASH", "RANDOM"])
    normalized_type : t.Optional[str]
        Target type for normalization:
        - "str": Python string (default)
        - "identifier": exp.Identifier
        - "literal": exp.Literal.string()
        - "column": exp.Column
        - "ast_expr": generic exp.Expression (defaults to Identifier)
    case_sensitive : bool
        Whether to perform case-sensitive matching (default: False)

    Validation: Checks if value is in allowed set, returns canonical string.
    Normalization: Converts to specified target type.
    """

    def __init__(self, valid_values: t.Sequence[str], normalized_type: str = "str", case_sensitive: bool = False):
        self.valid_values = list(valid_values)
        self.case_sensitive = bool(case_sensitive)
        self.normalized_type = normalized_type

        if self.normalized_type is not None and self.normalized_type not in PROPERTY_OUTPUT_TYPES:
            raise ValueError(
                f"normalized_type must be one of {PROPERTY_OUTPUT_TYPES}, got {self.normalized_type!r}"
            )

        # Pre-compute normalized values for efficient lookup
        self._values_normalized = [
            v if case_sensitive else v.upper() for v in self.valid_values
        ]

    def _extract_text(self, value: t.Any) -> t.Optional[str]:
        """Extract text from various value types."""
        if isinstance(value, str):
            return value
        if isinstance(value, exp.Literal):
            # For Literal, this is the actual value
            return str(value.this)
        if isinstance(value, (exp.Identifier, exp.Column)):
            # For Identifier/Column, this might be another Expression
            if isinstance(value.this, str):
                return value.this
            elif hasattr(value.this, 'name'):  # noqa: RET505
                return value.this.name
            else:
                return str(value.this)
        return None

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison based on case sensitivity."""
        return text if self.case_sensitive else text.upper()

    def validate(self, value: t.Any) -> t.Optional[str]:
        """Check if value is in the allowed enum set. Returns canonical string or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                parsed = parse_fragment(value)
                # If parsed successfully, extract text from AST node
                if isinstance(parsed, (exp.Identifier, exp.Literal, exp.Column)):
                    value = parsed
            except Exception:
                # If parsing fails, treat as plain string
                pass

        # Extract text from value
        text = self._extract_text(value)

        if text is None:
            return None

        # Normalize and check against allowed values
        normalized_text = self._normalize_text(text)
        if normalized_text in self._values_normalized:
            return normalized_text

        return None

    def normalize(self, validated: str) -> Normalized:
        """Convert validated enum string to target type."""
        # validated is already canonical (e.g., "HASH")
        if self.normalized_type is None or self.normalized_type == "str":
            return validated
        if self.normalized_type == "literal":
            return exp.Literal.string(validated)
        if self.normalized_type == "identifier":
            return exp.Identifier(this=validated)
        if self.normalized_type == "column":
            return exp.Column(this=validated)
        if self.normalized_type == "ast_expr":
            return exp.Identifier(this=validated)

        # Fallback to string
        return validated


class FuncType(DeclarativeType):
    """
    Function type validator.

    Accepts:
    - exp.Func (built-in functions like date_trunc, CAST, etc.)
    - exp.Anonymous (custom/dialect functions like RANGE, LIST)
    - String that can be parsed as function call

    Validation: Returns exp.Func or exp.Anonymous if valid, None otherwise.
    Normalization: Returns the function expression as-is (identity).

    Examples:
        date_trunc('day', col1)     → exp.Func
        RANGE(col1, col2)           → exp.Anonymous
        LIST(region, status)        → exp.Anonymous
    """

    def validate(self, value: t.Any) -> t.Optional[t.Union[exp.Func, exp.Anonymous]]:
        """Check if value is a function type. Returns exp.Func/exp.Anonymous or None."""
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Check if it's a Func or Anonymous function
        if isinstance(value, (exp.Func, exp.Anonymous)):
            return value

        return None

    def normalize(self, validated: t.Union[exp.Func, exp.Anonymous]) -> t.Union[exp.Func, exp.Anonymous]:
        """Return function expression as-is (identity normalization)."""
        return validated


# ============================================================
# AnyOf (combinator)
# ============================================================
class AnyOf(DeclarativeType):
    """
    Union type - accepts first matching sub-type.

    This is a combinator type that tries each sub-type in order and accepts
    the first one that validates successfully.

    Validation: Tries each sub-type, returns (matched_type, validated_value) tuple.
    Normalization: Uses the matched sub-type's normalize method.
    """

    def __init__(self, *types: DeclarativeType):
        if not types:
            raise ValueError("AnyOf requires at least one type")

        # Validate all types are DeclarativeType instances
        for t in types:
            if not isinstance(t, DeclarativeType):
                raise TypeError(f"AnyOf expects DeclarativeType instances, got {t!r}")

        self.types: t.List[DeclarativeType] = list(types)

    def validate(self, value: t.Any) -> t.Optional[t.Tuple[DeclarativeType, Validated]]:
        """Try each sub-type in order, return (matched_type, validated_value) or None."""
        for sub_type in self.types:
            validated = sub_type.validate(value)
            if validated is not None:
                # Return both the matched type and validated value
                return (sub_type, validated)

        # No type matched
        return None

    def normalize(self, validated: t.Tuple[DeclarativeType, Validated]) -> Normalized:
        """Normalize using the matched sub-type's normalize method."""
        matched_type, validated_value = validated
        return matched_type.normalize(validated_value)


# ============================================================
# SequenceOf (Tuple/List/Paren/Single -> normalized list)
# ============================================================
class SequenceOf(DeclarativeType):
    """
    Sequence/List type validator with built-in union type support.

    Accepts various sequence representations and validates each element against
    one or more possible types (similar to AnyOf for each element).
    Optionally accepts single elements (promoted to single-item lists).

    Accepts:
    - exp.Tuple: (a, b, c)
    - exp.Array: [a, b, c]
    - exp.Paren: (a) or ((a, b))
    - Python list/tuple: [a, b] or (a, b)
    - String: "a, b, c" (parsed)
    - Single element: a (if allow_single=True, promoted to [a])

    Validation: Returns list of (matched_type, validated_value) tuples or None.
    Normalization: Returns list of normalized elements using matched type's normalize.

    Examples:
        # Single type
        SequenceOf(ColumnType())

        # Multiple types (union) - each element tries types in order
        SequenceOf(ColumnType(), IdentifierType(), LiteralType())

        # Allow single element
        SequenceOf(ColumnType(), allow_single=True)

        # Multiple types + allow single
        SequenceOf(ColumnType(), IdentifierType(), allow_single=True)
    """

    def __init__(self, *elem_types: DeclarativeType, allow_single: bool = False, output_as: str = "list"):
        """
        Args:
            *elem_types: One or more type validators for elements.
                        If multiple types provided, each element tries types in order (AnyOf behavior).
            allow_single: Whether to accept single elements (promoted to list). Default: False.
            output_as: Output format - "list" or "tuple". Default: "list".
        """
        if not elem_types:
            raise ValueError("SequenceOf requires at least one element type")

        self.elem_types: t.List[DeclarativeType] = list(elem_types)
        self.allow_single = allow_single
        self.output_as = output_as

    def validate(self, value: t.Any) -> t.Optional[t.List[t.Tuple[DeclarativeType, Validated]]]:
        """Validate each element in the sequence. Returns list of (matched_type, validated_value) tuples or None."""
        # Extract elements from various container types
        elems = self._extract_elements(value)
        if elems is None:
            return None

        # Validate each element against all possible types (AnyOf behavior)
        validated_items: t.List[t.Tuple[DeclarativeType, Validated]] = []
        for elem in elems:
            # Try each type until one matches
            matched = False
            for elem_type in self.elem_types:
                validated = elem_type.validate(elem)
                if validated is not None:
                    validated_items.append((elem_type, validated))
                    matched = True
                    break

            # If no type matched, the whole sequence fails if any element fails
            if not matched:
                return None

        return validated_items

    def normalize(self, validated: t.List[t.Tuple[DeclarativeType, Validated]]) -> t.Union[t.List[Normalized], t.Tuple[Normalized, ...]]:
        """Normalize each validated element using its matched type's normalize method."""
        normalized_items = [elem_type.normalize(value) for elem_type, value in validated]

        # Convert to desired output format
        if self.output_as == "tuple":
            return tuple(normalized_items)
        return normalized_items  # default: list

    def _extract_elements(self, value: t.Any) -> t.Optional[t.List[t.Any]]:
        """
        Extract elements from various container representations.
        Returns list of raw elements or None if extraction fails.
        """
        # Python list/tuple - process first before string parsing
        if isinstance(value, (list, tuple)):
            return list(value)

        # Try parsing string for AST types
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                # If parsing fails and we accept single strings, promote to list
                if self.allow_single and any(isinstance(t, StringType) for t in self.elem_types):
                    return [value]
                return None

        # SQL Tuple: (a, b, c)
        if isinstance(value, exp.Tuple):
            return list(value.expressions)

        # SQL Array: [a, b, c]
        if isinstance(value, exp.Array):
            return list(value.expressions)

        # SQL Paren: (a) or ((a, b))
        if isinstance(value, exp.Paren):
            inner = value.this
            if isinstance(inner, exp.Tuple):
                return list(inner.expressions)
            return [inner]

        # Single AST element: promote to list (if allow_single)
        if self.allow_single and isinstance(value, exp.Expression):
            return [value]

        return None


# ============================================================
# Field Definition for Structured Types
# ============================================================
class Field:
    """
    Field specification for StructuredTupleType.

    Defines validation rules, types, and metadata for a single field.

    Args:
        type: DeclarativeType instance for validating field value
        required: Whether this field is required (default: False)
        aliases: List of alternative field names (default: [])
        doc: Documentation string for this field

    Example:
        Field(
            type=EnumType(["HASH", "RANDOM"]),
            required=True,
            aliases=["distribution_type"],
            doc="Distribution kind: HASH or RANDOM"
        )
    """

    def __init__(
        self,
        type: DeclarativeType,
        required: bool = False,
        aliases: t.Optional[t.List[str]] = None,
        doc: t.Optional[str] = None
    ):
        self.type = type
        self.required = required
        self.aliases = aliases or []
        self.doc = doc


# ============================================================
# StructuredTupleType - Base class for typed tuples
# ============================================================
class StructuredTupleType(DeclarativeType):
    """
    Base class for validating tuples with typed fields.

    Subclasses define FIELDS dict to specify structure:

    FIELDS = {
        "field_name": Field(
            type=SomeType(),
            required=True,
            aliases=["alt_name1", "alt_name2"]
        ),
        ...
    }

    Validation Process:
    1. Parse tuple into key=value pairs (exp.EQ)
    2. Match keys against FIELDS (including aliases)
    3. Validate each field value with specified type
    4. Check required fields are present
    5. Handle unknown/invalid fields based on error flags

    Returns: Dict[str, Any] with canonical field names as keys

    Example:
        class DistributionTupleType(StructuredTupleType):
            FIELDS = {
                "kind": Field(type=EnumType(["HASH", "RANDOM"]), required=True),
                "columns": Field(type=SequenceOf(ColumnType())),
            }

    Args:
        error_on_unknown_field: If True, raise error when encountering unknown fields.
                                If False, silently skip unknown fields (default: False)
        error_on_invalid_field: If True, raise error when field value validation fails.
                                If False, return None for entire validation (default: True)
    """

    FIELDS: t.Dict[str, Field] = {}  # Subclasses override this

    def __init__(self, error_on_unknown_field: bool = True, error_on_invalid_field: bool = True):
        self.error_on_unknown_field = error_on_unknown_field
        self.error_on_invalid_field = error_on_invalid_field

        # Build alias mapping: alias -> canonical_name
        self._alias_map: t.Dict[str, str] = {}
        for field_name, field_spec in self.FIELDS.items():
            # Map canonical name to itself
            self._alias_map[field_name] = field_name
            # Map aliases to canonical name
            for alias in field_spec.aliases:
                self._alias_map[alias] = field_name

    def validate(self, value: t.Any) -> t.Optional[t.Dict[str, t.Tuple[DeclarativeType, Validated]]]:
        """
        Validate structured tuple.

        Returns: Dict mapping canonical field names to (matched_type, validated_value) tuples,
                 or None if validation fails.

        Raises:
            ValueError: If error_on_unknown_field=True and unknown field encountered
            ValueError: If error_on_invalid_field=True and field validation fails
        """
        # Try parsing string first
        if isinstance(value, str):
            try:
                value = parse_fragment(value)
            except Exception:
                return None

        # Extract key=value pairs from tuple/paren
        pairs = self._extract_pairs(value)
        if pairs is None:
            return None

        # Validate each pair and build result dict
        result: t.Dict[str, t.Tuple[DeclarativeType, Validated]] = {}
        eq_type = EqType()

        for pair_expr in pairs:
            # Validate as EQ expression
            eq_validated = eq_type.validate(pair_expr)
            if eq_validated is None:
                continue  # Skip non-EQ expressions

            key, value_expr = eq_validated

            # Resolve alias to canonical name
            canonical_name = self._alias_map.get(key)
            if canonical_name is None:
                # Unknown field
                if self.error_on_unknown_field:
                    raise ValueError(
                        f"Unknown field '{key}' in {self.__class__.__name__}. "
                        f"Valid fields: {list(self.FIELDS.keys())}"
                    )
                # Skip unknown field
                continue

            # Get field spec
            field_spec = self.FIELDS[canonical_name]

            # Validate field value with specified type
            validated_value = field_spec.type.validate(value_expr)
            if validated_value is None:
                # Field validation failed
                if self.error_on_invalid_field:
                    raise ValueError(
                        f"Invalid value for field '{canonical_name}': {value_expr}. "
                        f"Expected type: {field_spec.type.__class__.__name__}"
                    )
                # Return None for entire validation
                return None

            # Store with canonical name
            result[canonical_name] = (field_spec.type, validated_value)

        # Check required fields
        for field_name, field_spec in self.FIELDS.items():
            if field_spec.required and field_name not in result:
                # Required field missing
                if self.error_on_invalid_field:
                    raise ValueError(
                        f"Required field '{field_name}' is missing in {self.__class__.__name__}"
                    )
                return None

        return result

    def normalize(self, validated: t.Dict[str, t.Tuple[DeclarativeType, Validated]]) -> t.Dict[str, Normalized]:
        """
        Normalize validated fields.

        Returns: Dict mapping canonical field names to normalized values.
        """
        return {
            field_name: field_type.normalize(value)
            for field_name, (field_type, value) in validated.items()
        }

    def _extract_pairs(self, value: t.Any) -> t.Optional[t.List[t.Any]]:
        """
        Extract list of expressions from tuple/paren.
        Each expression should be an exp.EQ (key=value).
        """
        # exp.Tuple: (a=1, b=2)
        if isinstance(value, (exp.Tuple, list)):
            return list(value.expressions)

        # exp.Paren: (a=1) or ((a=1, b=2))
        if isinstance(value, exp.Paren):
            inner = value.this
            if isinstance(inner, exp.Tuple):
                return list(inner.expressions)
            return [inner]

        return None


class DistributionTupleType(StructuredTupleType):
    """
    StarRocks distribution tuple validator.

    Accepts:
    - (kind='HASH', columns=(id, dt), buckets=10)
    - (kind='HASH', expressions=(id, dt), bucket_num=10)
    - (kind='RANDOM')

    Returns: Dict with fields:
        - kind: "HASH" or "RANDOM" (string)
        - columns: List[exp.Column] (optional, for HASH)
        - buckets: exp.Literal (optional)

    Field Aliases:
        - columns: expressions
        - buckets: bucket, bucket_num

    Examples:
        Input:  (kind='HASH', columns=(id, dt), buckets=10)
        Output: {
            'kind': 'HASH',
            'columns': [exp.Column('id'), exp.Column('dt')],
            'buckets': exp.Literal.number(10)
        }

        Input:  (kind='RANDOM')
        Output: {'kind': 'RANDOM'}

    Conversion:
        Use factory methods to convert normalized values to unified dict format:
        - from_enum(): Convert EnumType normalized value (str) → dict
        - from_func(): Convert FuncType normalized value (exp.Func) → dict
        - to_unified_dict(): Convert any normalized value → dict
    """

    FIELDS = {
        "kind": Field(
            type=EnumType(["HASH", "RANDOM"], normalized_type="str"),
            required=True,
            doc="Distribution type: HASH or RANDOM"
        ),
        "columns": Field(
            type=SequenceOf(ColumnType(), IdentifierType(normalized_type="column")),
            required=False,
            aliases=["expressions"],
            doc="Columns for HASH distribution"
        ),
        "buckets": Field(
            type=AnyOf(LiteralType(), StringType()),
            required=False,
            aliases=["bucket", "bucket_num"],
            doc="Number of buckets"
        )
    }

    # ============================================================
    # Factory methods for conversion from other normalized types
    # ============================================================

    @staticmethod
    def from_enum(enum_value: str, buckets: t.Optional[int] = None) -> t.Dict[str, t.Any]:
        """
        Create distribution dict from EnumType normalized value.

        Args:
            enum_value: "RANDOM" (from EnumType)
            buckets: Optional bucket count

        Returns:
            Dict with kind/columns/buckets fields

        Example:
            >>> DistributionTupleType.from_enum("RANDOM")
            {"kind": "RANDOM", "columns": [], "buckets": None}
        """
        return {
            "kind": enum_value,
            "columns": [],
            "buckets": buckets
        }

    @staticmethod
    def from_func(func: t.Union[exp.Func, exp.Anonymous], buckets: t.Optional[int] = None) -> t.Dict[str, t.Any]:
        """
        Create distribution dict from FuncType normalized value.

        Args:
            func: HASH(id, dt) or RANDOM() (from FuncType)
            buckets: Optional bucket count

        Returns:
            Dict with kind/columns/buckets fields

        Example:
            >>> func = parse_one("HASH(id, dt)")
            >>> DistributionTupleType.from_func(func)
            {"kind": "HASH", "columns": [exp.Column("id"), exp.Column("dt")], "buckets": None}
        """
        func_name = func.name.upper() if hasattr(func, 'name') else str(func.this).upper()

        if func_name == "HASH":
            # Extract columns from HASH(col1, col2, ...)
            columns = list(func.args) if hasattr(func, 'args') else []
            return {
                "kind": "HASH",
                "columns": columns,
                "buckets": buckets
            }
        elif func_name == "RANDOM":  # noqa: RET505
            return {
                "kind": "RANDOM",
                "columns": [],
                "buckets": buckets
            }
        else:
            raise ValueError(f"Unknown distribution function: {func_name}")

    @staticmethod
    def to_unified_dict(normalized_value: t.Any, buckets: t.Optional[t.Any] = None) -> t.Dict[str, t.Any]:
        """
        Convert any normalized distribution value to unified dict format.

        This is a convenience method that dispatches to appropriate factory method.

        Args:
            normalized_value: Result from DistributedBySpec normalization
                             (dict | str | exp.Func)
            buckets: Optional bucket count override

        Returns:
            Unified dict with kind/columns/buckets fields

        Raises:
            TypeError: If value type is not supported

        Example:
            >>> # From DistributionTupleType
            >>> DistributionTupleType.to_unified_dict({"kind": "HASH", "columns": [...]})
            {"kind": "HASH", "columns": [...], "buckets": None}

            >>> # From EnumType
            >>> DistributionTupleType.to_unified_dict("RANDOM")
            {"kind": "RANDOM", "columns": [], "buckets": None}

            >>> # From FuncType
            >>> DistributionTupleType.to_unified_dict(parse_one("HASH(id)"))
            {"kind": "HASH", "columns": [exp.Column("id")], "buckets": None}
        """
        if isinstance(normalized_value, dict):
            # Already in DistributionTupleType format
            return normalized_value
        elif isinstance(normalized_value, str):  # noqa: RET505
            # From EnumType: "RANDOM"
            return DistributionTupleType.from_enum(normalized_value, buckets)
        elif isinstance(normalized_value, (exp.Func, exp.Anonymous)):
            # From FuncType: HASH(id, dt)
            return DistributionTupleType.from_func(normalized_value, buckets)
        else:
            raise TypeError(
                f"Cannot convert {type(normalized_value).__name__} to distribution dict. "
                f"Expected dict, str, or exp.Func/exp.Anonymous."
            )


# ============================================================
# Type Specifications for StarRocks Properties
# ============================================================

class StarRocksPropertySpecs():

    # Accepts:
    # - Single column: id
    # - Multiple columns: (id, dt)
    GeneralColumnListInputSpec = SequenceOf(
        ColumnType(),
        StringType(normalized_type="column"),
        IdentifierType(normalized_type="column"),
        allow_single=True
    )

    # TableKey: Simple key specification (primary_key, duplicate_key, unique_key, aggregate_key)
    # Accepts:
    # - Single column: id
    # - Multiple columns: (id, dt)
    TableKeySpec = GeneralColumnListInputSpec

    # Partitioned By: Flexible partition specification
    # Accepts:
    # - Single column: col1
    # - Multiple columns: (col1, col2)
    # - Mixed: (col1, "col2") - string will be parsed
    # - RANGE(col1) or RANGE(col1, col2)
    # - LIST(col1) or LIST(col1, col2)
    # - Expression: (date_trunc('day', col1), col2)
    PartitionedBySpec = SequenceOf(
        ColumnType(),
        StringType(normalized_type="column"),
        IdentifierType(normalized_type="column"),
        FuncType(),     # RANGE(), LIST(), date_trunc(), etc.
        allow_single=True
    )

    # Partitions: List of partition definitions (strings)
    # Accepts:
    # - Single partition: 'PARTITION p1 VALUES LESS THAN ("2024-01-01")'
    # - Multiple partitions: ('PARTITION p1 ...', 'PARTITION p2 ...')
    # Note: Single string is auto-promoted to list
    PartitionsSpec = SequenceOf(StringType(), allow_single=True)

    # Distribution: StarRocks distribution specification
    # Accepts:
    # - Structured tuple1: (kind='HASH', columns=(id, dt), buckets=10)
    # - Structured tuple2: (kind='RANDOM')
    # - String format: "HASH(id)", "RANDOM", or "(kind='HASH', columns=(id), buckets=10)"
    # Note: Does NOT accept simple columns like id or (id, dt)
    #    And it can't directly accept "HASH(id) BUCKETS 10", you need to split it with "BUCKETS" to two parts.
    DistributedBySpec = AnyOf(
        DistributionTupleType(),  # Try structured tuple first (most specific)
        EnumType(["RANDOM"], normalized_type="str"),  # "RANDOM"
        FuncType(),  # "HASH(id)",
    )

    # OrderBy: Simple ordering specification
    # Accepts:
    # - Single column: dt
    # - Multiple columns: (dt, id, status)
    OrderBySpec = GeneralColumnListInputSpec

    # Generic property value: Accepts various types, normalizes to string
    # For properties like replication_num, storage_medium, etc.
    # StarRocks PROPERTIES syntax requires all values to be strings: "value"
    # So we normalize everything to string for consistent SQL generation
    GenericPropertyType = AnyOf(
        StringType(),     # Plain strings
        LiteralType(normalized_type="str"),    # Numbers and string literals → will be converted to string
        IdentifierType(normalized_type="str"), # Identifiers → will be converted to string
    )

    """
    Input Property Specification for StarRocks

    This specification defines the validation and normalization rules for StarRocks properties.
    Properties are specified in the physical_properties block of a SQLMesh model.

    Supported properties:
    - partitioned_by / partition_by: Partition specification
    - partitions: List of partition definitions
    - distributed_by: Distribution specification (HASH/RANDOM with structured tuple or string)
    - order_by: Ordering specification (simple column list)
    - table key:
        - primary_key: Primary key columns
        - duplicate_key: Duplicate key columns (for DUPLICATE KEY table)
        - unique_key: Unique key columns (for UNIQUE KEY table)
        - aggregate_key: Aggregate key columns (for AGGREGATE KEY table)
    - other properties: Any other properties not listed above will be treated as generic
                        string properties (e.g., replication_num, storage_medium, etc.)

    Examples:
        duplicate_key = dt                             # Single key
        primary_key = (id, customer_id)                # Multiple keys

        partitioned_by = col1                          # Single column
        partitioned_by = (col1, col2)                  # Multiple columns
        partitioned_by = (col1, "col2")                # Mixed (string will be parsed)
        partitioned_by = date_trunc('day', col1)       # Expression partition with single func
        partitioned_by = (date_trunc('day', col1), col2)  # Expression partition with multiple exprs
        partitioned_by = RANGE(col1, col2)             # RANGE partition
        partitioned_by = LIST(region, status)          # LIST partition

        distributed_by = (kind='HASH', columns=(id, dt), buckets=10)  # Structured
        distributed_by = (kind='RANDOM')               # RANDOM distribution
        distributed_by = "HASH(id)"                    # String format
        distributed_by = "RANDOM"                      # String format

        order_by = dt                                  # Single column
        order_by = (dt, id, status)                    # Multiple columns

        replication_num = 3                            # Generic property (auto-handled)
        storage_medium = "SSD"                         # Generic property (auto-handled)
    """
    PROPERTY_INPUT_SPEC: t.Dict[str, DeclarativeType] = {
        # Table key properties
        "primary_key": TableKeySpec,
        "duplicate_key": TableKeySpec,
        "unique_key": TableKeySpec,
        "aggregate_key": TableKeySpec,

        # Partition-related properties
        "partitioned_by": PartitionedBySpec,
        # "partition_by": PartitionedBySpec,  # Alias for partitioned_by
        "partitions": PartitionsSpec,

        # Distribution property
        "distributed_by": DistributedBySpec,

        # Ordering property
        "order_by": OrderBySpec,

        # Note: All other properties not listed here will be handled
        # by default GenericPropertyType (see get_property_input_type method)
    }


    GeneralColumnListOutputSpec = SequenceOf(ColumnType(), allow_single=False)

    """
    Output Property Specification for StarRocks after validation+normalization

    This specification describes the expected types after normalization.
    For most properties, OUTPUT spec is the same as INPUT spec since normalization
    preserves the diverse types (dict | str | exp.Func for distribution).

    Conversion to unified formats (e.g., all distributions → dict) happens separately
    in the usage layer via factory methods like DistributionTupleType.to_unified_dict().

    Expected Output Types (after normalization):
    - table keys: List[exp.Expression] - columns
    - partitioned_by: List[exp.Expression] - columns, functions
    - partitions: List[str] - partition definition strings
    - distributed_by: Dict | str | exp.Func - DistributionTupleType, EnumType, or FuncType output
    - order_by: List[exp.Expression] - columns
    - generic properties: str - normalized string values
    """
    PROPERTY_OUTPUT_SPEC: t.Dict[str, DeclarativeType] = {
        "primary_key": GeneralColumnListOutputSpec,
        "duplicate_key": GeneralColumnListOutputSpec,
        "unique_key": GeneralColumnListOutputSpec,
        "aggregate_key": GeneralColumnListOutputSpec,
        "partitioned_by": SequenceOf(ColumnType(), FuncType(), allow_single=False),
        "partitions": PartitionsSpec,
        "distributed_by": DistributedBySpec,  # Still dict | str | exp.Func after normalize
        "order_by": GeneralColumnListOutputSpec,
        # Generic properties use GenericPropertyType
    }


    # ============================================================
    # Helper functions
    # ============================================================

    @staticmethod
    def get_property_input_type(property_name: str) -> DeclarativeType:
        """
        Get the INPUT type validator for a property.

        Returns the specific type from PROPERTY_INPUT_SPEC if defined,
        otherwise returns GenericPropertyType for unknown properties.

        This allows any property not explicitly defined to be treated
        as a generic string property.
        """
        return StarRocksPropertySpecs.PROPERTY_INPUT_SPEC.get(property_name, StarRocksPropertySpecs.GenericPropertyType)

    @staticmethod
    def get_property_output_type(property_name: str) -> DeclarativeType:
        """
        Get the OUTPUT type validator for a property.

        Returns the specific type from PROPERTY_OUTPUT_SPEC if defined,
        otherwise returns GenericPropertyType for unknown properties.

        This allows validating that normalized values conform to expected output types.
        """
        return StarRocksPropertySpecs.PROPERTY_OUTPUT_SPEC.get(property_name, StarRocksPropertySpecs.GenericPropertyType)

    @staticmethod
    def validate_and_normalize_property(property_name: str, value: t.Any) -> t.Any:
        """
        Complete property processing pipeline:
        1. Get INPUT type validator
        2. Validate and normalize input value
        3. Get OUTPUT type validator
        4. Verify normalized output conforms to expected type
        5. Return verified output

        Raises:
            ValueError: If validation fails

        Example:
            >>> validated = validate_and_normalize_property("distributed_by", "RANDOM")
            >>> # Result: "RANDOM" (string from EnumType)
            >>>
            >>> validated = validate_and_normalize_property("distributed_by", "HASH(id)")
            >>> # Result: exp.Func (from FuncType)
        """
        # Step 1: Get INPUT type validator
        input_type = StarRocksPropertySpecs.get_property_input_type(property_name)

        # Step 2: Validate
        validated = input_type.validate(value)
        if validated is None:
            raise ValueError(
                f"Invalid value for property '{property_name}': {value!r}. "
                f"Expected type: {input_type.__class__.__name__}"
            )

        # Step 3: Normalize
        normalized = input_type.normalize(validated)

        # Step 4: Return
        return normalized


###############################################################################
@set_catalog()
class StarRocksEngineAdapter(
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    NonTransactionalTruncateMixin,
):
    """
    StarRocks Engine Adapter for SQLMesh.

    StarRocks is a high-performance analytical database that forked from Apache Doris.
    This adapter references the Doris implementation but differs in the following key areas:

    Key Differences from Doris:
    1. PRIMARY KEY Support:
       - StarRocks: Native PRIMARY KEY support (no conversion needed)
       - Doris: Uses UNIQUE KEY (requires conversion in _create_table_from_columns)

    2. DELETE with Subquery:
       - StarRocks PRIMARY KEY tables: Support DELETE with subqueries directly
       - Doris: Requires DELETE...USING syntax workaround

    3. Partition Types:
       - RANGE Partition: PARTITION BY RANGE(col) - with RANGE keyword
       - LIST Partition: PARTITION BY LIST(col) - with LIST keyword
       - Expression Partition: PARTITION BY (col1, col2) - no keyword

    Implementation Strategy:
    - Reference Doris implementation patterns (not inheriting)
    - Only implement methods that differ from base or Doris behavior
    - Most methods can use base class implementation directly

    Decision Tree for Method Overriding (see starrocks_design.md Appendix A.3):
    1. Does StarRocks syntax differ from standard SQL? → Override
    2. Does StarRocks behave differently from Doris? → Override
    3. Does base implementation handle it correctly? → Don't override
    4. Is it a Doris-specific workaround not needed in StarRocks? → Don't override
    """

    # ==================== Class Attributes (Declarative Configuration) ====================

    DIALECT = "starrocks"
    """SQLGlot dialect name for SQL generation"""

    DEFAULT_BATCH_SIZE = 5000
    """Default batch size for bulk operations"""

    SUPPORTS_TRANSACTIONS = False
    """
    StarRocks does not support transactions for multiple DML statements.
    - No BEGIN/COMMIT/ROLLBACK (only txn for multiple INSERT statements from v3.5)
    - Operations are auto-committed
    - Backfill uses partition-level atomicity
    """

    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    """
    StarRocks does support INSERT OVERWRITE syntax (dynamic overwrite from v3.5).
    Use DELETE + INSERT pattern:
    1. DELETE FROM table WHERE condition
    2. INSERT INTO table SELECT ...

    Base class automatically handles this strategy without overriding insert methods.

    TOD: later, we can add support for INSERT OVERWRITE, even use Primary Key for beter performance
    """

    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_CTAS
    """Table comments are added in both CREATE TABLE statement and CTAS"""

    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    """View comments are added in CREATE VIEW statement"""

    MAX_TABLE_COMMENT_LENGTH = 2048
    """Maximum length for table comments"""

    MAX_COLUMN_COMMENT_LENGTH = 255
    """Maximum length for column comments"""

    SUPPORTS_INDEXES = True
    """
    StarRocks supports PRIMARY KEY in CREATE TABLE, but NOT standalone CREATE INDEX.

    We set this to True to enable PRIMARY KEY generation in CREATE TABLE statements.
    The create_index() method is overridden to prevent actual CREATE INDEX execution.

    Supported (defined in CREATE TABLE):
    - PRIMARY KEY: Automatically creates sorted index
    - INDEX clause: For bloom filter, bitmap, inverted indexes

    Example:
        CREATE TABLE t (
            id INT,
            name STRING,
            INDEX idx_name (name) USING BITMAP
        ) PRIMARY KEY (id);  -- ✅ Supported

    NOT supported:
        CREATE INDEX idx_name ON t (name);  -- ❌ Will be skipped by create_index()

    """

    SUPPORTS_REPLACE_TABLE = False
    """No REPLACE TABLE syntax; use DROP + CREATE instead"""

    MAX_IDENTIFIER_LENGTH = 64
    """Maximum length for table/column names"""

    SUPPORTS_MATERIALIZED_VIEWS = True
    """StarRocks supports materialized views with refresh strategies"""

    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    """Materialized views can't have explicit schema definitions"""

    SUPPORTS_CREATE_DROP_CATALOG = False
    """StarRocks supports DROPing external catalogs.
    TODO: whether it's external catalogs, or includes the internal catalog
    """

    SUPPORTS_TUPLE_IN = False
    """
    StarRocks does NOT support tuple IN syntax: (col1, col2) IN ((val1, val2), (val3, val4))

    Instead, use OR with AND conditions:
    (col1 = val1 AND col2 = val2) OR (col1 = val3 AND col2 = val4)

    This is automatically handled by snapshot_id_filter and snapshot_name_version_filter
    in sqlmesh/core/state_sync/db/utils.py when SUPPORTS_TUPLE_IN = False.
    """

    # ==================== Schema Operations ====================
    # StarRocks supports CREATE/DROP SCHEMA the same as CREATE/DROP DATABSE.
    # So, no need to implement create_schema / drop_schema

    # ==================== Index Operations ====================

    def create_index(
        self,
        table_name: TableName,
        index_name: str,
        columns: t.Tuple[str, ...],
        exists: bool = True,
    ) -> None:
        """
        Override to prevent CREATE INDEX statements (not supported in StarRocks).

        StarRocks does not support standalone CREATE INDEX statements.
        Indexes must be defined during CREATE TABLE using INDEX clause.

        Since SQLMesh state tables use PRIMARY KEY (which provides efficient indexing),
        we simply log and skip additional index creation requests.

        This is a known limitation also present in Doris, which incorrectly allows
        CREATE INDEX to be attempted.

        Args:
            table_name: The name of the target table
            index_name: The name of the index
            columns: The list of columns that constitute the index
            exists: Indicates whether to include the IF NOT EXISTS check
        """
        logger.info(
            f"Skipping CREATE INDEX {index_name} on {table_name} - "
            "StarRocks does not support standalone CREATE INDEX statements. "
            "PRIMARY KEY provides equivalent indexing for columns: {columns}"
        )
        return

    def delete_from(
        self, table_name: TableName, where: t.Optional[t.Union[str, exp.Expression]] = None
    ) -> None:
        """
        Delete from a table.
        
        StarRocks has limitations:
        1. WHERE TRUE is not supported - use TRUNCATE TABLE instead
        2. More complex WHERE conditions may have limitations
        
        Args:
            table_name: The table to delete from
            where: The where clause to filter rows to delete
        """
        # Parse where clause if it's a string
        if isinstance(where, str):
            from sqlglot import parse_one
            where = parse_one(where, dialect=self.dialect)
        
        # If no where clause or WHERE TRUE, use TRUNCATE TABLE
        if not where or where == exp.true():
            table_expr = exp.to_table(table_name) if isinstance(table_name, str) else table_name
            logger.info(
                f"Converting DELETE FROM {table_name} WHERE TRUE to TRUNCATE TABLE "
                "(StarRocks does not support WHERE TRUE in DELETE)"
            )
            self.execute(f"TRUNCATE TABLE {table_expr.sql(dialect=self.dialect, identify=True)}")
            return
        
        # For other conditions, use parent implementation
        super().delete_from(table_name, where)

    def _where_clause_remove_boolean_literals(self, expression: exp.Expression) -> exp.Expression:
        """
        Remove TRUE/FALSE boolean literals from WHERE expressions.

        StarRocks doesn't support boolean literals in WHERE clauses.
        This method simplifies expressions like:
        - (condition) AND TRUE -> condition
        - (condition) OR FALSE -> condition
        - TRUE AND (condition) -> condition
        - WHERE TRUE -> 1=1 (though this case is handled by TRUNCATE conversion)
        - WHERE FALSE -> 1=0

        Args:
            expression: The expression to clean

        Returns:
            Cleaned expression without boolean literals
        """
        def transform(node: exp.Expression) -> exp.Expression:
            # Handle standalone TRUE/FALSE at the top level
            if node == exp.true():
                # Convert TRUE to 1=1
                return exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(1))
            elif node == exp.false():
                # Convert FALSE to 1=0
                return exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(0))

            # Handle AND expressions
            elif isinstance(node, exp.And):
                left = node.this
                right = node.expression

                # Remove TRUE from AND
                if left == exp.true():
                    return right
                if right == exp.true():
                    return left

            # Handle OR expressions
            elif isinstance(node, exp.Or):
                left = node.this
                right = node.expression

                # Remove FALSE from OR
                if left == exp.false():
                    return right
                if right == exp.false():
                    return left

            return node

        # Transform the expression tree
        return expression.transform(transform, copy=True)

    def _where_clause_convert_between_to_comparison(self, expression: exp.Expression) -> exp.Expression:
        """
        Convert BETWEEN expressions to >= AND <= comparisons.

        StarRocks DUPLICATE KEY tables don't support BETWEEN in DELETE statements.
        This method converts:
        - col BETWEEN a AND b  ->  col >= a AND col <= b

        Args:
            expression: The expression potentially containing BETWEEN

        Returns:
            Expression with BETWEEN converted to comparisons
        """
        def transform(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Between):
                # Extract components: col BETWEEN low AND high
                column = node.this  # The column being tested
                low = node.args.get("low")  # Lower bound
                high = node.args.get("high")  # Upper bound

                if column and low and high:
                    # Build: column >= low AND column <= high
                    gte = exp.GTE(this=column.copy(), expression=low.copy())
                    lte = exp.LTE(this=column.copy(), expression=high.copy())
                    return exp.And(this=gte, expression=lte)

            return node

        # Transform the expression tree
        return expression.transform(transform, copy=True)

    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        track_rows_processed: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        Override execute to strip FOR UPDATE from queries (not supported in StarRocks).

        StarRocks is an OLAP database and does not support row-level locking via
        SELECT ... FOR UPDATE. This method removes lock expressions before execution.

        Args:
            expressions: SQL expression(s) to execute
            ignore_unsupported_errors: Whether to ignore unsupported errors
            quote_identifiers: Whether to quote identifiers
            track_rows_processed: Whether to track rows processed
            **kwargs: Additional arguments
        """
        from sqlglot.helper import ensure_list

        # Process expressions to remove FOR UPDATE
        processed_expressions = []
        for e in ensure_list(expressions):
            if isinstance(e, exp.Expression):
                # Remove lock (FOR UPDATE) from SELECT statements
                if isinstance(e, exp.Select) and e.args.get("locks"):
                    e = e.copy()
                    e.set("locks", None)
                processed_expressions.append(e)
            else:
                # For string SQL, we can't easily remove FOR UPDATE without parsing
                # Just pass through and let StarRocks reject it if present
                processed_expressions.append(e)

        # Call parent execute with processed expressions
        super().execute(
            processed_expressions,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
            track_rows_processed=track_rows_processed,
            **kwargs,
        )

    # ==================== Table Creation (CORE IMPLEMENTATION) ====================




    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional["IntervalUnit"] = None,
        clustered_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, t.Any]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """
        Build table properties for StarRocks CREATE TABLE statement.

        Handles:
        - Table comment
        - Partition expressions (RANGE/LIST/EXPRESSION)
        - Distribution (HASH/RANDOM)
        - Order by (clustering)
        - Other properties (replication_num, storage_medium, etc.)

        Args:
            clustered_by: Clustering columns (generates ORDER BY clause)
            table_properties: Dictionary containing:
                - duplicate_key: Tuple/list of column names for DUPLICATE KEY
                - aggregate_key: Tuple/list of column names for AGGREGATE KEY
                - unique_key: Tuple/list of column names for UNIQUE KEY
                - distributed_by: Tuple of EQ expressions (kind, expressions, buckets)
                - partitions: Tuple of partition definition strings
                - order_by: Alias for clustered_by (backward compatibility)
                - replication_num, storage_medium, etc.: Literal values
        """
        properties: t.List[exp.Expression] = []
        table_properties_copy = dict(table_properties) if table_properties else {}

        # 1. Add table comment
        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        # 2. Handle key constraints (DUPLICATE KEY, AGGREGATE KEY, UNIQUE KEY)
        # Note: PRIMARY KEY is handled by base class via primary_key parameter
        self._add_key_properties(properties, table_properties_copy)

        # 3. Handle partitioned_by (PARTITION BY RANGE/LIST/EXPRESSION)
        partition_prop = self._build_partition_property(
            partitioned_by, partition_interval_unit, target_columns_to_types, catalog_name
        )
        if partition_prop:
            properties.append(partition_prop)

        # 4. Handle distributed_by (DISTRIBUTED BY HASH/RANDOM)
        distributed_prop = self._build_distribution_property(table_properties_copy)
        if distributed_prop:
            properties.append(distributed_prop)

        # 5. Handle order_by/clustered_by (ORDER BY ...)
        order_prop = self._build_order_by_property(table_properties_copy, clustered_by)
        if order_prop:
            properties.append(order_prop)

        # 6. Handle other properties (replication_num, storage_medium, etc.)
        other_props = self._build_other_properties(table_properties_copy)
        properties.extend(other_props)

        return exp.Properties(expressions=properties) if properties else None

    def _add_key_properties(
        self,
        properties: t.List[exp.Expression],
        table_properties: t.Dict[str, t.Any]
    ) -> None:
        """
        Add key constraint properties (DUPLICATE KEY, AGGREGATE KEY, UNIQUE KEY) to the properties list.

        Note: PRIMARY KEY is handled by base class via primary_key parameter.

        Args:
            properties: List to append key properties to
            table_properties: Dictionary containing key definitions (will be modified)
        """
        # Handle DUPLICATE KEY
        duplicate_key = table_properties.pop("duplicate_key", None)
        if duplicate_key is not None:
            key_columns = self._expr_to_column_tuple(duplicate_key)
            properties.append(
                exp.DuplicateKeyProperty(
                    expressions=[exp.to_column(col) for col in key_columns]
                )
            )

        # Handle UNIQUE KEY (legacy, prefer PRIMARY KEY in StarRocks 3.0+)
        unique_key = table_properties.pop("unique_key", None)
        if unique_key is not None:
            key_columns = self._expr_to_column_tuple(unique_key)
            properties.append(
                exp.UniqueKeyProperty(
                    expressions=[exp.to_column(col) for col in key_columns]
                )
            )

        # Note: AGGREGATE KEY not implemented yet - requires column aggregation functions

    def _build_partition_property(
        self,
        partitioned_by: t.Optional[t.List[exp.Expression]],
        partition_interval_unit: t.Optional["IntervalUnit"],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        catalog_name: t.Optional[str]
    ) -> t.Optional[exp.Expression]:
        """
        Build partition property expression.

        StarRocks supports:
        - PARTITION BY RANGE (cols) - for time-based partitions
        - PARTITION BY LIST (cols) - for categorical partitions
        - PARTITION BY (exprs) - for expression partitions
        - PARTITION BY exprs - for expression partitions, without `(`, `)`

        Args:
            partitioned_by: Partition column expressions
            partition_interval_unit: Optional time unit for automatic partitioning
            target_columns_to_types: Column definitions
            catalog_name: Catalog name (if applicable)

        Returns:
            Partition property expression or None
        """
        if not partitioned_by:
            return None

        # Use base class implementation if available
        return self._build_partitioned_by_exp(
            partitioned_by,
            partition_interval_unit=partition_interval_unit,
            target_columns_to_types=target_columns_to_types,
            catalog_name=catalog_name,
        )

    def _build_distribution_property(
        self,
        table_properties: t.Dict[str, t.Any]
    ) -> t.Optional[exp.DistributedByProperty]:
        """
        Build DISTRIBUTED BY property from table_properties.

        Supports:
        - DISTRIBUTED BY HASH (col1, col2, ...) BUCKETS n
        - DISTRIBUTED BY RANDOM

        Args:
            table_properties: Dictionary containing distributed_by (will be modified)

        Returns:
            DistributedByProperty or None
        """
        distributed_by = table_properties.pop("distributed_by", None)
        if distributed_by is None:
            return None

        # Parse the Tuple of EQ expressions
        distributed_info = {}
        # like: (kind = 'HASH', expressions = 'id', buckets = 8)
        if isinstance(distributed_by, exp.Tuple):
            for expr in distributed_by.expressions:
                if isinstance(expr, exp.EQ) and hasattr(expr.this, "this"):
                    # Remove quotes from the key if present
                    key = str(expr.this.this).strip('"')
                    # string style distribution value
                    if isinstance(expr.expression, exp.Literal):
                        distributed_info[key] = expr.expression.this
                    # a single column, a tuple of columns. converted to a list of column names
                    elif isinstance(expr.expression, exp.Column):
                        distributed_info[key] = [expr.expression.name]
                    elif isinstance(expr.expression, exp.Tuple):
                        distributed_info[key] = [
                            e.name if isinstance(e, exp.Column) else str(e)
                            for e in expr.expression.expressions
                        ]
                    else:
                        distributed_info[key] = expr.expression

        # Build DistributedByProperty
        if distributed_info:
            kind = str(distributed_info.get("kind", "RANDOM"))
            expressions = distributed_info.get("expressions", [])
            if not isinstance(expressions, list):
                expressions = [expressions] if expressions else []

            buckets = distributed_info.get("buckets")

            return exp.DistributedByProperty(
                kind=exp.Var(this=kind),
                expressions=[
                    exp.to_column(e) if not isinstance(e, exp.Expression) else e
                    for e in expressions
                ],
                buckets=exp.Literal.number(int(buckets)) if buckets else None,
                order=None,
            )

        return None

    def _build_order_by_property(
        self,
        table_properties: t.Dict[str, t.Any],
        clustered_by: t.Optional[t.List[exp.Expression]]
    ) -> t.Optional[exp.Cluster]:
        """
        Build ORDER BY (clustering) property.

        Supports both:
        - clustered_by parameter (from create_table call)
        - order_by in table_properties (backward compatibility alias)

        Priority: clustered_by parameter > order_by in table_properties

        Args:
            table_properties: Dictionary containing optional order_by (will be modified)
            clustered_by: Clustering columns from parameter

        Returns:
            Cluster expression (generates ORDER BY) or None
        """
        # Support order_by as an alias for clustered_by
        order_by = table_properties.pop("order_by", None)
        if order_by is not None and clustered_by is None:
            # Convert order_by to clustered_by format
            if isinstance(order_by, exp.Tuple):
                clustered_by = order_by.expressions
            elif isinstance(order_by, (list, tuple)):
                clustered_by = [
                    exp.to_column(col) if not isinstance(col, exp.Expression) else col
                    for col in order_by
                ]
            elif isinstance(order_by, exp.Column):
                clustered_by = [order_by]
            elif isinstance(order_by, str):
                clustered_by = [exp.to_column(order_by)]

        if clustered_by:
            return exp.Cluster(expressions=clustered_by)

        return None

    def _build_other_properties(
        self,
        table_properties: t.Dict[str, t.Any]
    ) -> t.List[exp.Property]:
        """
        Build other literal properties (replication_num, storage_medium, etc.).

        Args:
            table_properties: Dictionary containing properties (will be modified)

        Returns:
            List of Property expressions
        """
        other_props = []
        for key, value in list(table_properties.items()):
            # Skip special keys handled elsewhere
            if key in ("partitions", "duplicate_key", "unique_key", "aggregate_key",
                      "distributed_by", "order_by"):
                continue

            # Convert value to Property
            if isinstance(value, exp.Literal):
                other_props.append(
                    exp.Property(this=exp.to_identifier(key), value=value)
                )
            elif isinstance(value, (str, int, float)):
                other_props.append(
                    exp.Property(
                        this=exp.to_identifier(key),
                        value=exp.Literal.string(str(value))
                    )
                )

        return other_props

    def _create_table_from_columns(
        self,
        table_name: TableName,
        target_columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Create a table using column definitions.

        StarRocks Supports PRIMARY KEY natively
          CREATE TABLE t (id INT) PRIMARY KEY(id)
          → Extract primary_key from table_properties and pass to base class

        StarRocks Key Column Ordering Constraint:
        ALL key types (PRIMARY KEY, UNIQUE KEY, DUPLICATE KEY, AGGREGATE KEY) require:
        - Key columns MUST be the first N columns in CREATE TABLE
        - Column order MUST match the KEY clause order
        - Example: PRIMARY KEY(order_id, event_date) requires:
          CREATE TABLE t (
            order_id INT,      -- ✅ 1st column matches 1st key
            event_date DATE,   -- ✅ 2nd column matches 2nd key
            customer_id INT,   -- ✅ Other columns follow
            ...
          )

        Implementation:
        1. Priority: Parameter primary_key > table_properties primary_key
        2. Extract key columns from table_properties (primary_key, unique_key, duplicate_key, aggregate_key)
        3. Validate no conflicts between different key types
        4. Reorder target_columns_to_types to place key columns first
        5. Pass to base class for SQL generation

        Args:
            table_name: Fully qualified table name
            target_columns_to_types: Column definitions {name: DataType}
            primary_key: Primary key column names (parameter takes priority)
            exists: Add IF NOT EXISTS clause
            table_description: Table comment
            column_descriptions: Column comments {column_name: comment}
            kwargs: Additional properties (partitioned_by, distributed_by, etc.)

        Example:
            # In MODEL:
            physical_properties (
                primary_key = (order_id, event_date),
                distributed_by = (kind='HASH', expressions=customer_id, buckets=10)
            )

            # Generates:
            CREATE TABLE IF NOT EXISTS db.sales (
                order_id INT,
                event_date DATE,
                customer_id INT
            )
            PRIMARY KEY(order_id, event_date)
            DISTRIBUTED BY HASH(customer_id) BUCKETS 10
        """
        # Use setdefault to simplify table_properties access
        table_properties = kwargs.setdefault("table_properties", {})

        # Extract and validate key columns from table_properties
        # Priority: parameter primary_key > table_properties
        key_type, key_columns = self._extract_and_validate_key_columns(
            table_properties, primary_key
        )

        # Update primary_key based on extracted key type
        if key_type == "primary_key":
            primary_key = key_columns
        elif key_type in ("unique_key", "duplicate_key", "aggregate_key"):
            # For other key types, columns still need reordering but handled differently
            # These will be processed by _build_table_properties_exp()
            primary_key = None  # Don't generate PRIMARY KEY clause
        else:
            # No key defined
            primary_key = None
            key_columns = None

        # StarRocks key column ordering constraint: All key types need reordering
        if key_columns:
            target_columns_to_types = self._reorder_columns_for_key(
                target_columns_to_types, key_columns, key_type or "key"
            )

        # Pass to base class (will generate PRIMARY KEY if primary_key is set)
        super()._create_table_from_columns(
            table_name=table_name,
            target_columns_to_types=target_columns_to_types,
            primary_key=primary_key,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def _extract_and_validate_key_columns(
        self,
        table_properties: t.Dict[str, t.Any],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> t.Tuple[t.Optional[str], t.Optional[t.Tuple[str, ...]]]:
        """
        Extract and validate key columns from table_properties.

        StarRocks Table Types and Key Requirements:
        1. PRIMARY KEY table - primary_key property (StarRocks 3.0+)
        2. UNIQUE KEY table - unique_key property (legacy, replacable by PK)
        3. DUPLICATE KEY table - duplicate_key property
        4. AGGREGATE KEY table - aggregate_key property

        All key types require:
        - Key columns must be the first N columns in CREATE TABLE
        - Column order must match the KEY clause order

        Priority:
        - Parameter primary_key > table_properties primary_key
        - Only one key type allowed per table

        Args:
            table_properties: Table properties dictionary (lowercase keys expected)
            primary_key: Primary key from method parameter (highest priority)

        Returns:
            Tuple of (key_type, key_columns)
            - key_type: One of 'primary_key', 'unique_key', 'duplicate_key', 'aggregate_key', None
            - key_columns: Tuple of column names, or None

        Raises:
            SQLMeshError: If multiple key types are defined or column extraction fails
        """
        # Check which key types are present (keys are lowercase in table_properties)
        key_types_present = []
        for key_type in ["primary_key", "unique_key", "duplicate_key", "aggregate_key"]:
            if key_type in table_properties:
                key_types_present.append(key_type)

        # Validate only one key type in table_properties
        if len(key_types_present) > 1:
            raise SQLMeshError(
                f"Multiple key types defined in table_properties: {key_types_present}. "
                "Only one key type is allowed per table."
            )

        # Priority: parameter primary_key > table_properties
        if primary_key:
            # If parameter is provided and table_properties also has a key, warn
            if key_types_present:
                logger.warning(
                    f"Both parameter primary_key and table_properties {key_types_present[0]} "
                    f"are defined. Parameter primary_key takes priority: {primary_key}"
                )
                # Remove from table_properties to avoid duplicate (only for primary_key parameter case)
                table_properties.pop(key_types_present[0], None)
            return ("primary_key", primary_key)

        # Extract from table_properties
        if not key_types_present:
            return (None, None)

        # For other 3 table types
        key_type = key_types_present[0]
        key_expr = table_properties[key_type]  # Read without popping - needed later!

        # Convert expression to tuple of column names
        key_columns = self._expr_to_column_tuple(key_expr)

        logger.info(f"Extracted {key_type} from table_properties: {key_columns}")

        return (key_type, key_columns)

    def _expr_to_column_tuple(
        self, expr: t.Any
    ) -> t.Tuple[str, ...]:
        """
        Convert various expression types to tuple of column names.

        Handles:
        - exp.Tuple: Tuple of Column expressions
        - list/tuple: List of Column expressions or strings
        - exp.Column: Single column
        - str: Single column name

        Args:
            expr: Expression to convert

        Returns:
            Tuple of column names

        Raises:
            SQLMeshError: If expression type is unsupported
        """
        if isinstance(expr, exp.Tuple):
            # exp.Tuple with Column expressions
            return tuple(col.name for col in expr.expressions)
        elif isinstance(expr, (list, tuple)):  # noqa: RET505
            # List/tuple of expressions or strings
            return tuple(
                col.name if isinstance(col, exp.Column) else str(col)
                for col in expr
            )
        elif isinstance(expr, exp.Column):
            # Single column
            return (expr.name,)
        elif isinstance(expr, str):
            # Single column name as string
            return (expr,)
        else:
            raise SQLMeshError(
                f"Unsupported key column expression type: {type(expr)}. "
                f"Expected exp.Tuple, list, tuple, exp.Column, or str."
            )

    def _reorder_columns_for_key(
        self,
        target_columns_to_types: t.Dict[str, exp.DataType],
        key_columns: t.Tuple[str, ...],
        key_type: str = "key",
    ) -> t.Dict[str, exp.DataType]:
        """
        Reorder columns to place key columns first.

        StarRocks Constraint (ALL Table Types):
        Key columns (PRIMARY/UNIQUE/DUPLICATE/AGGREGATE) MUST be the first N columns
        in the CREATE TABLE statement, in the same order as defined in the KEY clause.

        Example:
            Input:
                columns = {"customer_id": INT, "order_id": BIGINT, "event_date": DATE}
                key_columns = ("order_id", "event_date")
                key_type = "primary_key"

            Output:
                {"order_id": BIGINT, "event_date": DATE, "customer_id": INT}

        Args:
            target_columns_to_types: Original column order (from SELECT)
            key_columns: Key column names in desired order
            key_type: Type of key for logging (primary_key, unique_key, etc.)

        Returns:
            Reordered columns with key columns first

        Raises:
            SQLMeshError: If a key column is not found in target_columns_to_types
        """
        # Validate that all key columns exist
        missing_key_cols = set(key_columns) - set(target_columns_to_types.keys())
        if missing_key_cols:
            raise SQLMeshError(
                f"{key_type} columns {missing_key_cols} not found in table columns. "
                f"Available columns: {list(target_columns_to_types.keys())}"
            )

        # Build new ordered dict: key columns first, then remaining columns
        reordered = {}

        # 1. Add key columns in key order
        for key_col in key_columns:
            reordered[key_col] = target_columns_to_types[key_col]

        # 2. Add remaining columns (preserve original order)
        for col_name, col_type in target_columns_to_types.items():
            if col_name not in key_columns:
                reordered[col_name] = col_type

        logger.info(
            f"Reordered columns for {key_type.upper()}: "
            f"Original order: {list(target_columns_to_types.keys())}, "
            f"New order: {list(reordered.keys())}"
        )

        return reordered


    # ==================== Methods to Consider Overriding (Future Implementation) ====================

    # TODO: _get_data_objects()
    # Purpose: Query information_schema to list tables/views
    # Override Decision: Probably NO - Doris implementation should work (MySQL-compatible)
    # Reference: doris.py uses information_schema.tables

    # TODO: create_view()
    # Purpose: Create regular and materialized views
    # Override Decision: MAYBE - Check if StarRocks materialized view syntax differs from Doris
    # Reference: doris.py has _create_materialized_view() for complex properties

    # TODO: delete_from()
    # Purpose: Delete rows from table
    # Override Decision: MAYBE
    # - StarRocks PRIMARY KEY tables support DELETE with subqueries (no USING workaround needed)
    # - Other table types (DUPLICATE/AGGREGATE) may still need Doris-style handling
    # Reference: doris.py converts DELETE...IN (SELECT) to DELETE...USING

    # TODO: _build_table_properties_exp()
    # Purpose: Build table properties (PARTITION BY, DISTRIBUTED BY, etc.)
    # Override Decision: MAYBE
    # - Check if StarRocks partition syntax (RANGE/LIST/Expression) needs special handling
    # - distributed_by, buckets should be same as Doris
    # Reference: doris.py has complex _build_table_properties_exp()

    # TODO: _build_partitioned_by_exp()
    # Purpose: Build partition expressions
    # Override Decision: MAYBE
    # - StarRocks has 3 partition types: RANGE, LIST, Expression
    # - May need special handling for expression partitions
    # Reference: doris.py handles RANGE and LIST partitions

    # TODO: create_table_like()
    # Purpose: CREATE TABLE ... LIKE ...
    # Override Decision: Probably NO - Standard syntax should work

    # TODO: _create_table_comment() / _build_create_comment_column_exp()
    # Purpose: Add/modify table and column comments
    # Override Decision: Probably NO - Doris implementation should work
    # Uses: ALTER TABLE ... MODIFY COMMENT / MODIFY COLUMN ... COMMENT

    # ==================== Methods NOT Needing Override (Base Class Works) ====================

    # The following methods work correctly with base class implementation:
    # - columns(): Query column definitions via DESCRIBE TABLE
    # - table_exists(): Check if table exists via information_schema
    # - insert_append(): Standard INSERT INTO ... SELECT
    # - insert_overwrite_by_time_partition(): Uses DELETE_INSERT strategy (handled by base)
    # - fetchall() / fetchone(): Standard query execution
    # - execute(): Base SQL execution
    # - create_table_properties(): Delegate to _build_table_properties_exp()

    # ==================== Notes on SQLGlot Support ====================

    # SQLGlot StarRocks Dialect Status:
    # - Location: sqlglot/dialects/starrocks.py
    # - Inheritance: Inherits from MySQL (not Doris!)
    # - PRIMARY KEY: Already supported (placed in POST_SCHEMA location)
    # - Partition expressions: Should be supported
    #
    # If SQLGlot is missing features, we have two options:
    # 1. Implement workaround in this adapter (temporary)
    # 2. Contribute to SQLGlot repository (long-term)
    #
    # See starrocks_design.md for detailed SQLGlot modification tracking
