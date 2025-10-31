/* Test RANGE partition parsing */
MODEL (
  name mytest.test_range_partition,
  kind FULL,
  dialect starrocks,
  partitioned_by RANGE(dt),
  physical_properties (
    distributed_by = (kind='HASH', expressions=id, buckets=10),
    replication_num = 1
  ),
  columns (
    id INT,
    dt DATE
  )
);

SELECT id, dt FROM source_table
