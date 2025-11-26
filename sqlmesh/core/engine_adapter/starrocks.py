from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

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
