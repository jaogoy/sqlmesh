/* Test distributed_by with structured Tuple syntax */
MODEL (
  name mytest.test_distribution_hash,
  kind FULL,
  dialect starrocks,

  physical_properties (
    -- Test structured distributed_by (recommended)
    /* distributed_by = (kind='HASH', expressions=id, buckets=10), */
    /* distributed_by = (kind='HASH', columns=id, bucket=10), */
    /* distributed_by = (kind='HASH', columns=(id, dt), bucket_num=10), */
    /* distributed_by = (kind='RANDOM'), */
    /* distributed_by = "(kind='HASH', columns=(id, dt), bucket_num=10)", */
    distributed_by = 10
  ),

  columns (
    id INT,
    dt DATE,
    amount DECIMAL(18,2)
  )
);

SELECT 1 AS id, CURRENT_DATE AS dt, 100.00 AS amount
