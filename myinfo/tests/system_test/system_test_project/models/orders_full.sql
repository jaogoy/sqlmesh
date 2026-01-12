MODEL (
  name starrocks_system_test.orders_full,
  kind FULL,
  -- Partition value-form coverage: RANGE single col (model parameter)
  partitioned_by RANGE(ds),
  -- Order-by value-form coverage: model-level clustered_by (tuple)
  clustered_by (order_id, ds),
  physical_properties (
    -- Key value-form coverage: string no-paren (auto-wrap)
    duplicate_key = "order_id, ts, region",
    partitions = (
      'PARTITION p1 VALUES LESS THAN ("2025-01-01")',
      'PARTITION p2 VALUES LESS THAN ("2025-02-01")',
      'PARTITION p3 VALUES LESS THAN ("2025-03-01")'
    ),
    -- Distribution value-form coverage: string RANDOM
    distributed_by = 'RANDOM',
    -- Generic properties value-form coverage
    replication_num = 1
  ),
  grain (order_id),
  owner system_test_owner,
  cron '@daily',
  start '2025-01-01',
  tags (system_test, full_load),
  columns (
    order_id INT,
    user_id INT,
    region VARCHAR(20),
    amount DECIMAL(10, 2),
    ts BIGINT,
    ds DATE
--    , comment VARCHAR(100)  -- add nullable column for ST-07
  )
);

SELECT
  order_id,
  user_id,
  region,
  amount,
  ts,
  ds
--  , NULL AS comment
FROM starrocks_system_test.raw_orders;
