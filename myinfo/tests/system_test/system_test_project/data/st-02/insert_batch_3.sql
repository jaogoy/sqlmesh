  -- ST-02 batch 3: new ds partitions (2025-02-01)
  --
  -- IMPORTANT: Replace the table name below with the actual physical table name.
  -- Run `find_physical_table.sql` first to get the correct table name.

INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467
  (order_id, user_id, region, amount, ts, ds) VALUES
  (15, 112, 'us',   15.00, 1738368000, '2025-02-01'),
  (16, 113, 'eu',   22.22, 1738368000, '2025-02-01'),
  (17, 114, 'apac', 88.88, 1738368000, '2025-02-01');


