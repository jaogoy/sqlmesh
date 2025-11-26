/* Test distributed_by with RANDOM */
MODEL (
  name mytest.test_distribution_random,
  kind FULL,
  dialect starrocks,

  physical_properties (
    -- Test RANDOM distribution (single value)
    distributed_by = (kind='RANDOM'),
  ),

  columns (
    id INT,
    dt DATE,
    value STRING
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt, 'test' AS value
