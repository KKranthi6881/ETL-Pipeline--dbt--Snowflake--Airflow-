-- get_top_sales.sql
-- Purpose: Retrieve top 10 orders ranked by total sales value from fact_orders
-- Expected: Highest grossing sales entries for quick insight

SELECT
    order_id,
    customer_id,
    order_date,
    total_sales_amount,
    discount_amount,
    (total_sales_amount - (discount_amount * 0.8)) AS net_sales,
    region,
    category
FROM {{ ref('fact_orders') }}
ORDER BY total_sales_amount DESC
LIMIT 10;