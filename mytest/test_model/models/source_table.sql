MODEL (
  name mytest.source_table,
  kind SEED (
    path '../seeds/source_table.csv'
  ),
  columns (
    order_id INT,
    customer_id INT,
    region VARCHAR(50),
    amount DECIMAL(18,2),
    status VARCHAR(20),
    event_date DATE
  ),
  grain (order_id, event_date)
);
