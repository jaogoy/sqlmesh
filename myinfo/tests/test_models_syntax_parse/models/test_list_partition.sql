/* Test 2: LIST partition with columns */
MODEL (
  name mytest.test_list_partition,
  kind FULL,
  partitioned_by (col1, col2),
  columns (
    id INT,
    col1 STRING,
    col2 STRING
  )
);

SELECT 1 AS id, 'a' AS col1, 'b' AS col2
