/* Test RANGE partition with multiple columns - Using physical_properties */
MODEL (
  name mytest.test_range_multi_columns,
  kind FULL,
  dialect starrocks,

  -- ❌ This DOES NOT work - validator rejects it:
  -- partitioned_by RANGE(col1, col2),  -- Error: contains multiple columns

  -- ✅ Solution: Use physical_properties instead
  physical_properties (
    partition_by = RANGE(col1, col2),
    -- distributed_by = (kind='HASH', expressions=id, buckets=10),
    -- replication_num = 1
  ),

  columns (
    id INT,
    col1 DATE,
    col2 STRING
  )
);

SELECT 1 AS id, CURRENT_DATE AS col1, 'test' AS col2
