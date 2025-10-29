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
