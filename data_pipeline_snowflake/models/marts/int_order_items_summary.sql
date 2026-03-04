select
    order_key,
    min(order_date) as order_date,
    sum(extended_price) as gross_item_sales_amount,
    avg(item_discount_amount) as avg_item_discount_amount,
    sum(item_discount_amount) as total_item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
