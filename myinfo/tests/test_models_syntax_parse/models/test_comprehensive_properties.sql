/* Test comprehensive physical_properties with all major properties */
MODEL (
  name mytest.test_comprehensive_properties,
  kind FULL,
  dialect starrocks,

  physical_properties (
    -- Table type
    duplicate_key = (dt, region, id),

    -- Partitioning
    partition_by = RANGE(dt, region),
    partitions = (
      'PARTITION p1 VALUES LESS THAN ("2025-01-01", "US")',
      'PARTITION p2 VALUES LESS THAN ("2025-01-01", "EU")',
      'PARTITION p3 VALUES LESS THAN ("2025-06-01", "US")',
      'PARTITION p4 VALUES LESS THAN (MAXVALUE, MAXVALUE)'
    ),

    -- Distribution
    distributed_by = (kind='HASH', expressions=(id, region), buckets=16),

    -- Ordering
    order_by = (dt, region, id),

    -- Other properties
    replication_num = 3,
    storage_medium = "SSD",
    storage_cooldown_time = "2025-12-31 23:59:59"
  ),

  columns (
    id INT,
    dt DATE,
    region STRING,
    status STRING,
    amount DECIMAL(18,2)
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt, 'US' AS region, 'active' AS status, 100.00 AS amount
