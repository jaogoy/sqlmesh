/* Test table types: duplicate_key, unique_key, aggregate_key */
MODEL (
  name mytest.test_table_keys,
  kind FULL,
  dialect starrocks,

  physical_properties (
    -- Test DUPLICATE KEY (most common for analytical workloads)
    /* duplicate_key = dt, */
    duplicate_key = (dt),
    /* duplicate_key = (dt, id), */
  ),

  columns (
    id INT,
    dt DATE,
    name STRING,
    amount DECIMAL(18,2)
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt, 'test' AS name, 100.00 AS amount
