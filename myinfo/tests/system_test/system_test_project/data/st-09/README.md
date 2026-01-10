# System Test Cases

## ST-09 duplicate-key data

Use `insert_duplicate_keys.sql` to insert rows that reuse existing `(order_id, ds)` keys without changing schema. This forces SQLMesh to perform delete+insert when `orders_incremental` is restated without a rebuild.

Recommended workflow:

1. Run this script after ST-03/04 so the referenced dates already exist.
2. Execute the ST-09 plan command (`--restate-model starrocks_system_test.orders_incremental` for the target window).
3. Validate that only duplicate keys are deleted/reinserted (no table rebuild).

