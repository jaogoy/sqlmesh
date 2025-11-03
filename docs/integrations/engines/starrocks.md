# StarRocks

## Overview

[StarRocks](https://www.starrocks.io/) is a next-generation sub-second MPP OLAP database designed for real-time analytics. It provides high concurrency, low latency, and supports both batch and stream processing.

SQLMesh supports StarRocks through its MySQL-compatible protocol, providing StarRocks-specific optimizations for table models, indexing, partitioning, and more. The adapter leverages StarRocks's strengths for analytical workloads with sensible defaults and advanced configuration support.

## Connection Configuration Example

```yaml
starrocks:
  connection:
    type: starrocks
    host: fe.starrocks.cluster  # Frontend (FE) node address
    port: 9030                  # Query port (default: 9030)
    user: starrocks_user
    password: your_password
    database: your_database
    # Optional MySQL-compatible settings
```

## Table Models

StarRocks supports four table models: DUPLICATE KEY, PRIMARY KEY, UNIQUE KEY, and AGGREGATE KEY. SQLMesh supports **DUPLICATE** and **PRIMARY KEY** models through the `grain` and `physical_properties` configuration.

### DUPLICATE Model (Default)

**Example Configuration:**
```sql
MODEL (
  name user_events,
  kind FULL,
  physical_properties (
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 10
    )
  )
);
```

### PRIMARY KEY Model

For INCREMENTAL models, **PRIMARY KEY tables are required** to support efficient DELETE operations during incremental updates.

**Example Configuration:**
```sql
MODEL (
  name user_events,
  kind INCREMENTAL_BY_TIME_RANGE(
    time_column event_date
  ),
  grain (user_id, event_date),  -- Defines PRIMARY KEY columns
  physical_properties (
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 16
    )
  )
);
```

The `grain` property automatically maps to `PRIMARY KEY` in StarRocks table creation.

## Limitations

### INCREMENTAL Models Require PRIMARY KEY

**IMPORTANT**: When using `INCREMENTAL_BY_TIME_RANGE` or `INCREMENTAL_BY_UNIQUE_KEY` model kinds with StarRocks:

- **DUPLICATE KEY tables do NOT support BETWEEN conditions in DELETE statements**
- SQLMesh automatically converts `BETWEEN` to `>= AND <=` comparisons, but DUPLICATE KEY tables have additional limitations
- **Solution**: Use PRIMARY KEY tables for incremental models by specifying the `grain` property

**Incorrect (will fail):**
```sql
MODEL (
  name user_events,
  kind INCREMENTAL_BY_TIME_RANGE(time_column event_date),
  -- Missing grain - creates DUPLICATE KEY table
  physical_properties (
    distributed_by (kind = 'HASH', expressions = 'user_id', buckets = 10)
  )
);
```

**Correct:**
```sql
MODEL (
  name user_events,
  kind INCREMENTAL_BY_TIME_RANGE(time_column event_date),
  grain (user_id, event_date),  -- Creates PRIMARY KEY table
  physical_properties (
    distributed_by (kind = 'HASH', expressions = 'user_id', buckets = 10)
  )
);
```

## Table Properties

The StarRocks adapter supports a comprehensive set of table properties that can be configured in the `physical_properties` section of your model.

### Core Table Properties

| Property         | Type   | Description                           | Example                          |
| ---------------- | ------ | ------------------------------------- | -------------------------------- |
| `distributed_by` | `Dict` | Distribution configuration            | See Distribution section         |
| `partitions`     | `Tuple[str]` or `str` | Custom partition expression | `('PARTITION p202401 VALUES LESS THAN ("2024-02-01")')` |

### Distribution Configuration

The `distributed_by` property supports multiple formats:

**Dictionary Format:**
```sql
MODEL (
  name my_table,
  kind FULL,
  physical_properties (
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 10
    )
  )
);
```

```sql
MODEL (
  name my_table,
  kind FULL,
  physical_properties (
    distributed_by (
      kind = 'RANDOM'
    )
  )
);
```

**Supported Distribution Types:**
- `HASH`: Hash-based distribution (most common)
- `RANDOM`: Random distribution

**Bucket Configuration:**
- Integer value: Fixed number of buckets (e.g., `10`)

### Partitioning

StarRocks supports range partitioning, list partitioning, and expression partitioning to improve query performance.

**Custom Partition Expression:**
```sql
MODEL (
  name my_partitioned_model,
  kind INCREMENTAL_BY_TIME_RANGE(time_column event_date),
  grain (user_id, event_date),
  partitioned_by (event_date),
  physical_properties (
    partitions = (
      'PARTITION p202401 VALUES LESS THAN ("2024-02-01")',
      'PARTITION p202402 VALUES LESS THAN ("2024-03-01")',
      'PARTITION p202403 VALUES LESS THAN ("2024-04-01")'
    ),
    distributed_by (kind = 'HASH', expressions = 'user_id', buckets = 10)
  )
);
```

### Generic Properties

Any additional properties in `physical_properties` are passed through as StarRocks table properties:

```sql
MODEL (
  name advanced_table,
  kind FULL,
  grain (id),
  physical_properties (
    distributed_by (
      kind = 'HASH',
      expressions = 'id',
      buckets = 8
    ),
    replication_num = '1',
    storage_medium = 'SSD',
    enable_persistent_index = 'true',
    compression = 'LZ4'
  )
);
```

## Comments

SQLMesh supports adding comments to tables and columns with automatic truncation to StarRocks limits.

- **Table Comments**: Use the `description` property in the `MODEL` definition
- **Column Comments**: Use the `column_descriptions` property in the `MODEL` definition

```sql
MODEL (
  name my_commented_table,
  kind TABLE,
  grain (id),
  description 'Comprehensive table for tracking user events',
  column_descriptions (
    id = "Unique identifier for each event",
    user_id = "Foreign key reference to users table",
    event_type = "Type of event that occurred"
  )
);
```

**Limits:**
- Table comments: 2048 characters (automatically truncated)
- Column comments: 1024 characters (automatically truncated)

## Dependencies

To use StarRocks with SQLMesh, install the required MySQL driver:

```bash
pip install "sqlmesh[starrocks]"
# or
pip install pymysql
```

## Resources

- [StarRocks Documentation](https://docs.starrocks.io/)
- [StarRocks Table Design Guide](https://docs.starrocks.io/docs/table_design/StarRocks_table_design/)
- [StarRocks SQL Reference](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/)
