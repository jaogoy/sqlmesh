-- Find the physical table name for raw_orders seed model
-- Run this in StarRocks SQL client to get the actual physical table name with hash

-- Method 1: Query virtual view definition
SHOW CREATE VIEW starrocks_system_test.raw_orders;

-- Method 2: List all tables matching the pattern
SHOW TABLES FROM sqlmesh__starrocks_system_test LIKE 'starrocks_system_test__raw_orders__%';

-- Method 3: Query information_schema
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_schema = 'sqlmesh__starrocks_system_test'
  AND table_name LIKE 'starrocks_system_test__raw_orders__%'
ORDER BY create_time DESC
LIMIT 1;


