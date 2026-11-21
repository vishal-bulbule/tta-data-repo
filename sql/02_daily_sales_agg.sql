-- Rebuild the daily sales mart.
-- Invoked by daily_sales_etl.build_daily_agg.

CREATE OR REPLACE TABLE `gcai-may-26.sales.daily_sales_agg` AS
SELECT
  DATE(o.order_ts)                      AS sales_date,
  o.store_id,
  COUNT(DISTINCT o.order_id)            AS order_count,
  SUM(o.amount)                         AS gross_revenue,
  SAFE_DIVIDE(SUM(o.amount), COUNT(DISTINCT o.order_id)) AS avg_order_value
FROM `gcai-may-26.sales.raw_orders` o
LEFT JOIN `gcai-may-26.sales.customer_lookup` c
  ON o.customer_id = c.customer_id
WHERE o.status = 'COMPLETED'
GROUP BY sales_date, o.store_id;
