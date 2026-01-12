MODEL (
  name starrocks_system_test.orders_incremental,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds,
    batch_size 7
  ),
  -- Partition value-form coverage: multi-expr with function (expression partition)
  partitioned_by (date_trunc('day', ds), region),
  -- Distribution value-form coverage: structured HASH (unquoted kind) + BUCKETS
  physical_properties (
    primary_key = (order_id, ds, region),
    distributed_by = (kind=HASH, expressions=order_id, buckets=2)
  ),
  grain (order_id, ds),
  owner system_test_owner,
  start '2025-01-01',
  cron '@daily',
  tags (system_test, incremental),
  columns (
    order_id INT,
    user_id INT,
    region VARCHAR(20),
    amount DECIMAL(10, 2),
    ts BIGINT,
    ds DATE
  )
);

SELECT
  order_id,
  user_id,
  region,
  amount,
  ts,
  ds
FROM starrocks_system_test.raw_orders
WHERE ds BETWEEN @start_ds AND @end_ds
