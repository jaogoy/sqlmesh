/* Test 1: partitions and other properties */
MODEL (
  name mytest.test_range_partition,
  kind FULL,

  partitioned_by RANGE(dt)
);

SELECT 1 AS id, CURRENT_DATE AS dt
