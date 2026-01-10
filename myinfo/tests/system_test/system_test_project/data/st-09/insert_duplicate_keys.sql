    -- ST-09 duplicate-key batch: reuse existing (order_id, ds) but change amounts to trigger delete+insert
    -- Run after ST-03/ST-04 so that 2025-02-03 already exists.

INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467
  (order_id, user_id, region, amount, ts, ds) VALUES
  (13, 111, 'apac', 99.99, 1736294400, '2025-01-08'),  -- overwrite existing key
  (8,  106, 'us',   33.33, 1736035200, '2025-01-05');

