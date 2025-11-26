/* Test 3: Partition with expression function */
MODEL (
  name mytest.test_expression_partition,
  kind FULL,
  partitioned_by date_trunc('day', created_at),
  columns (
    id INT,
    created_at TIMESTAMP
  )
);

SELECT 1 AS id, CURRENT_TIMESTAMP AS created_at
