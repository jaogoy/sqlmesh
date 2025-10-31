/* StarRocks Model with Complex Partition Expression */
MODEL (
  name mytest.starrocks_complex_partition,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    batch_size 30,
  ),
  owner test_user,
  dialect starrocks,
  partitioned_by (date_trunc('day', event_date), customer_id),
  physical_properties (
    distributed_by = (kind='HASH', expressions=order_id, buckets=10)
  ),
  columns (
    order_id INT,
    customer_id INT,
    event_date DATETIME
  )
);

SELECT
  order_id,
  customer_id,
  event_date
FROM source_table
WHERE event_date BETWEEN @start_date AND @end_date
