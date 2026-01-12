# System Test Cases

## ST-02 data batches (simulate upstream inserts)

These files simulate new upstream data arriving in `starrocks_system_test.raw_orders`.

### Two ways to insert data

**Option A: INSERT to physical table (recommended for system test)**

1. Find the physical table name: run `find_physical_table.sql` to get the hash-suffixed table name
2. Update `insert_batch_*.sql` to use the physical table name (replace `starrocks_system_test.raw_orders` with the actual physical table)
3. Run the updated insert script

**Option B: INSERT to virtual view (if supported)**

- Some databases allow INSERT to views, but StarRocks may not support this
- If virtual view is not updatable, you must use Option A

### After inserting

- Run `sqlmesh plan/apply` to validate incremental behavior
- Verify that only新 / restated `ds` 分区被处理（除非显式 `--restate-model`）
- 若需要 ST-04 的重叠数据，请使用 `data/st-04/insert_overlap_backfill.sql`

### Notes

- Physical table name format: `sqlmesh__starrocks_system_test.starrocks_system_test__raw_orders__<hash>`
- The hash may change if the model definition changes, but should be stable for system tests
- Virtual view points to physical table via `SELECT * FROM physical_table`, so INSERT to physical table is immediately visible

