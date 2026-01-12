MODEL (
  name starrocks_system_test.orders_full_4_evolution,
  kind FULL,
  partitioned_by (ds),
  grain (order_id, ds),
  owner system_test_owner,
  cron '@daily',
  start '2025-01-01',
  tags (system_test, evolution)
);

SELECT
  order_id,
  user_id,
  region,
  amount,
  ts,
  ds
FROM starrocks_system_test.orders_view;

