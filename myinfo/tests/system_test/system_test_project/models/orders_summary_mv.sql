MODEL (
  name starrocks_system_test.orders_summary_mv,
  kind VIEW (
    materialized true
  ),
  -- Keep MV simple: partition is a single column (expression partition coverage already in orders_full)
  partitioned_by (date_trunc('day', ds), region),
  owner system_test_owner,
  cron '@daily',
  start '2025-01-01',
  tags (system_test, mv),
  columns (
    ds DATE,
    user_id INT,
    region VARCHAR(20),
    order_cnt BIGINT,
    gross_amount DECIMAL(18, 2)
--     distinct_orders BIGINT,
--     gross_amount DECIMAL(18, 2),
--     gross_amount_expr DECIMAL(18, 2)
  ),
  physical_properties (
    -- Distribution value-form coverage: structured HASH (unquoted kind)
    distributed_by = (kind=HASH, expressions=user_id, buckets=4),
    -- MV refresh value-form coverage
    refresh_moment = DEFERRED,
    refresh_scheme = "ASYNC START ('2025-01-01 00:00:00') EVERY (INTERVAL 30 MINUTE)"
  )
);

SELECT
  ds,
  user_id,
  region,
  COUNT(*) AS order_cnt,
  SUM(amount) AS gross_amount
--  COUNT(DISTINCT order_id) AS distinct_orders,
--  SUM(amount) AS gross_amount,
--  SUM(amount_time3) AS gross_amount_expr
FROM starrocks_system_test.orders_incremental
GROUP BY ds, user_id, region;

