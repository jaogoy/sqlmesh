    -- ST-04 overlap batch: duplicate ds (2025-01-06) with modified values to trigger delete+insert
    -- Run this after ST-02 batches when testing destructive incremental/backfill flows.

INSERT INTO sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__3488006467
  (order_id, user_id, region, amount, ts, ds) VALUES
  (9,  107, 'eu',   75.55, 1736121600, '2025-01-06'),
  (10, 108, 'apac', 22.22, 1736121600, '2025-01-06');
