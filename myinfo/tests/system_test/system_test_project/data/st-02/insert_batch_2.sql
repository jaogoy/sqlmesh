  -- ST-02 batch 2: new ds partitions (2025-01-07, 2025-01-08)
  --
  -- IMPORTANT: Replace the table name below with the actual physical table name.
  -- Run `find_physical_table.sql` first to get the correct table name.

INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467
  (order_id, user_id, region, amount, ts, ds) VALUES
  (11, 109, 'eu',   99.95, 1736208000, '2025-01-07'),
  (12, 110, 'us',    7.00, 1736208000, '2025-01-07'),
  (13, 111, 'apac',  3.33, 1736294400, '2025-01-08'),
  (14, 111, 'apac', 40.40, 1736294400, '2025-01-08');


