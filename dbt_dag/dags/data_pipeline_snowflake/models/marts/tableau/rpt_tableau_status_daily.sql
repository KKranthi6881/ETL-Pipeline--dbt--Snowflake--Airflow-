select
    cast(order_date as date) as order_date,
    status_code,
    count(distinct order_key) as order_count,
    sum(total_price) as order_total_price_amount,
    sum(gross_item_sales_amount) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount,
    sum(gross_item_sales_amount + item_discount_amount) as net_sales_amount
from {{ ref('fact_orders') }}
group by 1, 2
order by 1, 2
