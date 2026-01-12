/* Test unknown function parsing */
MODEL (
  name mytest.test_unknown_func,
  kind FULL,
  dialect starrocks,
  partitioned_by (my_custom_func('day', dt), customer_id),
  physical_properties (
    distributed_by = (kind='HASH', expressions=id, buckets=10),
    replication_num = 1
  ),
  columns (
    id INT,
    customer_id INT,
    dt DATE
  )
);

SELECT id, customer_id, dt FROM source_table
