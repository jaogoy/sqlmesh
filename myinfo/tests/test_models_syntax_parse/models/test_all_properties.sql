/*
 * Comprehensive test model combining all StarRocks table properties
 * This model includes representative values for each property type
 */
MODEL (
  name mytest.test_all_properties,
  kind FULL,
  dialect starrocks,

  partition_by RANGE(dt, region),
  physical_properties (
    -- Table Key Type (determines how data is stored)
    primary_key = (dt, region, id),
    -- duplicate_key = (dt, region, id),
    -- unique_key = (id),
    -- aggregate_key = (dt, region),

    -- Partitioning Strategy
    -- partition_by = RANGE(dt, region),

    -- Partition Definitions (explicit partitions)
    partitions = (
      'PARTITION p1 VALUES LESS THAN ("2025-01-01", "US")',
      'PARTITION p2 VALUES LESS THAN ("2025-01-01", "EU")',
      'PARTITION p3 VALUES LESS THAN ("2025-06-01", "US")',
      'PARTITION p4 VALUES LESS THAN (MAXVALUE, MAXVALUE)'
    ),

    -- Distribution Strategy
    distributed_by = (kind='HASH', expressions=(id, region), buckets=16),
    -- Alternatives:
    -- distributed_by = (kind='RANDOM'),
    -- distributed_by = 10,  -- shorthand for 10 buckets

    -- Clustering/Ordering
    order_by = (dt, region, id, status),

    -- Storage Properties
    replication_num = 3,
    storage_medium = "SSD",
    storage_cooldown_time = "2025-12-31 23:59:59",

    -- Compression
    compression = "LZ4",

    -- Additional properties (optional)
    enable_persistent_index = true,
    bloom_filter_columns = "id,region"
  ),

  columns (
    id INT,
    dt DATE,
    region STRING,
    status STRING,
    amount DECIMAL(18,2),
    description STRING
  )
);

-- Sample data
SELECT
  1 AS id,
  CURRENT_DATE AS dt,
  'US' AS region,
  'active' AS status,
  100.00 AS amount,
  'Test record' AS description
UNION ALL
SELECT
  2 AS id,
  CURRENT_DATE AS dt,
  'EU' AS region,
  'inactive' AS status,
  200.50 AS amount,
  'Another test record' AS description
