select
    cast(order_date as date) as order_date,
    count(distinct order_key) as order_count,
    count(distinct customer_key) as customer_count,
    sum(total_price) as order_total_price_amount,
    sum(gross_item_sales_amount) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount,
    sum(gross_item_sales_amount + item_discount_amount) as net_sales_amount,
    avg(total_price) as avg_order_value_amount,
    case
        when sum(gross_item_sales_amount) = 0 then null
        else (-1 * sum(item_discount_amount)) / sum(gross_item_sales_amount)
    end as effective_discount_rate
from {{ ref('fact_orders') }}
group by 1
order by 1
