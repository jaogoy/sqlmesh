MODEL (
  name starrocks_system_test.orders_view,
  kind VIEW,
  owner system_test_owner,
  cron '@daily',
  start '2025-01-01',
  tags (system_test, reference),
  virtual_properties (
    -- Cover view SECURITY value form (identifier/unquoted)
    security = INVOKER
  )
);

SELECT
  order_id,
  user_id,
  region,
  amount,
  ts,
--   ts - 3600 as ts,  -- modify a column to propagate changes through DAG
  ds,
FROM starrocks_system_test.raw_orders;
