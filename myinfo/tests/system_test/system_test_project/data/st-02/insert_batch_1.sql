 -- ST-02 batch 1: new ds partitions (2025-01-05, 2025-01-06)
 --
 -- IMPORTANT: Replace the table name below with the actual physical table name.
 -- Run `find_physical_table.sql` first to get the correct table name.
 -- Format: sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__<hash>
 --
 -- Example (replace <hash> with actual hash):
 -- INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__<hash> ...

INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467
  (order_id, user_id, region, amount, ts, ds) VALUES
  (7,  106, 'us',   19.99, 1736035200, '2025-01-05'),
  (8,  106, 'us',    2.50, 1736035200, '2025-01-05'),
  (9,  107, 'eu',   60.00, 1736121600, '2025-01-06'),
  (10, 108, 'apac', 11.11, 1736121600, '2025-01-06');


