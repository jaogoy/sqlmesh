/* Comprehensive StarRocks Model with All Properties */
MODEL (
  name mytest.starrocks_comprehensive,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    batch_size 30,
  ),
  owner test_user,
  dialect starrocks,
  cron '@daily',
  start '2024-01-01',
  grain (order_id, event_date),
  partitioned_by (event_date),
  clustered_by (customer_id, region),
  physical_properties (
    distributed_by = (kind='HASH', expressions=customer_id, buckets=10),
    partitions = (
      'PARTITION p202401 VALUES LESS THAN ("2024-02-01")',
      'PARTITION p202402 VALUES LESS THAN ("2024-03-01")',
      'PARTITION p202403 VALUES LESS THAN ("2024-04-01")'
    ),
    replication_num = '3',
    storage_medium = 'SSD',
    enable_persistent_index = 'true',
    compression = 'LZ4'
  ),
  storage_format parquet,
  columns (
    order_id INT COMMENT 'Order ID',
    customer_id INT COMMENT 'Customer ID',
    region VARCHAR(50) COMMENT 'Region code',
    amount DECIMAL(18,2) COMMENT 'Order amount',
    status VARCHAR(20) COMMENT 'Order status',
    event_date DATE COMMENT 'Event date for partitioning'
  )
);

SELECT
  order_id,
  customer_id,
  region,
  amount,
  status,
  event_date
FROM source_table
WHERE event_date BETWEEN @start_date AND @end_date
