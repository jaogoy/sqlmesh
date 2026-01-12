-- Seed model backing all system tests
MODEL (
  name starrocks_system_test.raw_orders,
  kind SEED (
    path '../seeds/raw_orders.csv'
  ),
  grain order_id,
  owner system_test_owner,
  tags (system_test, seed),
  columns (
    order_id INT,
    user_id INT,
    region VARCHAR(20),
    amount DECIMAL(10, 2),
    ts BIGINT,
    ds DATE
  )
);
