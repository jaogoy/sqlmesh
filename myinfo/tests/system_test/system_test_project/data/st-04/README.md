# System test cases

## ST-04 overlap data

Purpose: generate conflicting rows for destructive incremental changes.

Steps:

1. Run `insert_overlap_backfill.sql` to overwrite existing 2025-01-06 rows in the physical `raw_orders` table.
2. Modify `orders_incremental` filters (see ST-04 case) so SQLMesh must restate that partition.
3. Execute the ST-04 plan command to validate delete+insert workflow.
