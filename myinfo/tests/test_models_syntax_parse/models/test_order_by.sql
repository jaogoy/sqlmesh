/* Test order_by with multiple columns */
MODEL (
  name mytest.test_order_by,
  kind FULL,
  dialect starrocks,

  physical_properties (
    -- Test order_by (alias for clustered_by in StarRocks)
    order_by = (dt, id, status),
  ),

  columns (
    id INT,
    dt DATE,
    status STRING,
    amount DECIMAL(18,2)
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt, 'active' AS status, 100.00 AS amount
