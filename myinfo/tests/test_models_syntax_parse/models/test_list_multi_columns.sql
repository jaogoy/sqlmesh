/* Test LIST partition with multiple columns via physical_properties */
MODEL (
  name mytest.test_list_multi_columns,
  kind FULL,
  dialect starrocks,

  -- Use physical_properties to test LIST with multiple columns
  physical_properties (
    partition_by = LIST(region, status),

    partitions = (
      'PARTITION p_us_active VALUES IN (("US", "active"), ("US", "pending"))',
      'PARTITION p_eu_active VALUES IN (("EU", "active"), ("EU", "pending"))',
      'PARTITION p_others VALUES IN (("ASIA", "active"), ("ASIA", "inactive"))'
    ),

    -- distributed_by = (kind='HASH', expressions=id, buckets=8),
    -- replication_num = 1
  ),

  columns (
    id INT,
    region STRING,
    status STRING,
    amount DECIMAL(18,2)
  )
);

SELECT 1 AS id, 'US' AS region, 'active' AS status, 100.00 AS amount
