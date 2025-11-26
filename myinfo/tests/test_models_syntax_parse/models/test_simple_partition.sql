/* Test 4: Simple column partition without parentheses */
MODEL (
  name mytest.test_simple_partition,
  kind FULL,
  partitioned_by dt,
  columns (
    id INT,
    dt DATE
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt
