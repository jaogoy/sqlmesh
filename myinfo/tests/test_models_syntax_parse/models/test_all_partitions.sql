/* Test various partition syntaxes via physical_properties */
MODEL (
  name mytest.test_all_partitions,
  kind FULL,

  -- partitioned_by col1,
  # partitioned_by RANGE(col1),
  # partitioned_by RANGE(col1, col2),  # Error: contains multiple columns
  # partitioned_by LIST(col1, col2),  # Error: contains multiple columns
  # partitioned_by (col1, col2, col3),
  /* partitioned_by col1, col2, col3, */
  # partitioned_by date_trunc('day', col1),
  # partitioned_by (date_trunc('day', col1), col2),
  /* partitioned_by date_trunc('day', col1), col2, */
  # partitioned_by (my_date_func(col1, "param1"), col2),
  # partitioned_by "RANGE(col1, col2)",
  partitioned_by RANGE("col1", col2),

  physical_properties (
    -- Test RANGE with multiple columns (bypasses validator)
    partition_by = RANGE(col1, col2),

    -- Partition definitions
    /* partitions = (
      'PARTITION p1 VALUES LESS THAN ("2024-01-01", "region1")',
      'PARTITION p2 VALUES LESS THAN ("2024-06-01", "region2")',
      'PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE)'
    ), */

    -- Distribution (required for StarRocks)
    -- distributed_by = (kind='HASH', expressions=id, buckets=10),

    -- replication_num = 1
  ),

  columns (
    id INT,
    col1 DATE,
    col2 STRING,
    col3 STRING
  )
);

SELECT 1 AS id, CURRENT_DATE AS col1, 'region1' AS col2, "abc" AS col3
