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
    set_catalog,
)

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
    """StarRocks supports secondary indexes (bloom filter, bitmap, etc.)"""

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

    # ==================== Schema Operations ====================
    # StarRocks supports CREATE/DROP SCHEMA the same as CREATE/DROP DATABSE.
    # So, no need to implement create_schema / drop_schema


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
        - Partition expressions (including RANGE/LIST)
        - Distribution (HASH/RANDOM)
        - Other properties (replication_num, storage_medium, etc.)
        
        Args:
            table_properties: Dictionary containing:
                - distributed_by: Tuple of EQ expressions (kind, expressions, buckets)
                - partitions: Tuple of partition definition strings
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
        
        # 2. Handle distributed_by (DISTRIBUTED BY HASH(...) BUCKETS n)
        distributed_by = table_properties_copy.pop("distributed_by", None)
        if distributed_by is not None:
            # Parse the Tuple of EQ expressions
            distributed_info = {}
            if isinstance(distributed_by, exp.Tuple):
                for expr in distributed_by.expressions:
                    if isinstance(expr, exp.EQ) and hasattr(expr.this, "this"):
                        key = str(expr.this.this).strip('"')
                        if isinstance(expr.expression, exp.Literal):
                            distributed_info[key] = expr.expression.this
                        elif isinstance(expr.expression, exp.Column):
                            # Single column
                            distributed_info[key] = [expr.expression]
                        else:
                            distributed_info[key] = expr.expression
            
            # Build DistributedByProperty
            if distributed_info:
                kind = distributed_info.get("kind", "HASH")
                expressions = distributed_info.get("expressions", [])
                if not isinstance(expressions, list):
                    expressions = [expressions] if expressions else []
                
                buckets = distributed_info.get("buckets")
                
                properties.append(
                    exp.DistributedByProperty(
                        kind=exp.Var(this=kind),  # Use Var for kind
                        expressions=[exp.to_column(e) if not isinstance(e, exp.Expression) else e 
                                   for e in expressions],
                        buckets=exp.Literal.number(buckets) if buckets else None,
                        order=None,
                    )
                )
        
        # 3. Handle partitioned_by (PARTITION BY RANGE/LIST/Expression)
        # Note: partitions are separate - they're the actual partition definitions
        # partitioned_by contains the column expressions
        
        # 4. Handle other properties (replication_num, storage_medium, etc.)
        # Collect all remaining literal properties into PROPERTIES(...)
        other_props = []
        for key, value in table_properties_copy.items():
            # Skip special keys that are handled elsewhere
            if key in ("partitions",):  # partitions handled separately
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
        
        if other_props:
            # Wrap in Properties node for PROPERTIES(...) syntax
            properties.append(
                exp.Properties(expressions=other_props)
            )
        
        return exp.Properties(expressions=properties) if properties else None

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

        Why Override:
        This is the CORE DIFFERENCE between StarRocks and Doris:

        - StarRocks: Supports PRIMARY KEY natively
          CREATE TABLE t (id INT) PRIMARY KEY(id)
          → Pass primary_key directly to base class

        - Doris: Uses UNIQUE KEY instead
          CREATE TABLE t (id INT) UNIQUE KEY(id)
          → Convert primary_key to unique_key in table_properties

        Implementation:
        - StarRocks can use base class implementation directly
        - Just pass primary_key as-is without conversion
        - Base class generates: PRIMARY KEY(col1, col2, ...)

        Reference: Doris converts primary_key → unique_key in table_properties
        See: doris.py _create_table_from_columns()

        Args:
            table_name: Fully qualified table name
            target_columns_to_types: Column definitions {name: DataType}
            primary_key: Primary key column names (StarRocks supports this!)
            exists: Add IF NOT EXISTS clause
            table_description: Table comment
            column_descriptions: Column comments {column_name: comment}
            kwargs: Additional properties (partitioned_by, distributed_by, etc.)

        Example:
            adapter._create_table_from_columns(
                table_name="db.sales",
                target_columns_to_types={
                    "id": exp.DataType.build("INT"),
                    "amount": exp.DataType.build("DECIMAL(18,2)"),
                    "dt": exp.DataType.build("DATE")
                },
                primary_key=("id",),
                table_properties={
                    "distributed_by": {...},
                    "partitioned_by": [exp.Column("dt")]
                }
            )

            Generates:
            CREATE TABLE IF NOT EXISTS db.sales (
                id INT,
                amount DECIMAL(18,2),
                dt DATE
            )
            PRIMARY KEY(id)
            PARTITION BY RANGE(dt) ()
            DISTRIBUTED BY HASH(id) BUCKETS 10
        """
        # StarRocks supports PRIMARY KEY natively - no conversion needed!
        # Just pass primary_key directly to base class
        super()._create_table_from_columns(
            table_name=table_name,
            target_columns_to_types=target_columns_to_types,
            primary_key=primary_key,  # Pass as-is (unlike Doris which converts to unique_key)
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

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
