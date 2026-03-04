with customer_rollup as (
    select
        customer_key,
        min(cast(order_date as date)) as first_order_date,
        max(cast(order_date as date)) as last_order_date,
        count(distinct order_key) as order_count,
        sum(total_price) as order_total_price_amount,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(gross_item_sales_amount + item_discount_amount) as net_sales_amount
    from {{ ref('fact_orders') }}
    group by 1
)
select
    customer_key,
    first_order_date,
    last_order_date,
    order_count,
    order_total_price_amount,
    gross_item_sales_amount,
    item_discount_amount,
    net_sales_amount,
    row_number() over (order by net_sales_amount desc) as net_sales_rank
from customer_rollup
order by net_sales_rank
