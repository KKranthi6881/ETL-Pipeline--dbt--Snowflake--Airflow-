select
    cast(order_date as date) as order_date,
    count(*) as total_rows,
    count_if(status_code not in ('P', 'O', 'F')) as unexpected_status_rows,
    count_if(total_price <= 0) as non_positive_total_price_rows,
    count_if(item_discount_amount > 0) as positive_discount_rows,
    count_if((gross_item_sales_amount + item_discount_amount) > gross_item_sales_amount) as net_sales_gt_gross_rows,
    count_if(customer_key is null) as null_customer_rows
from {{ ref('fact_orders') }}
group by 1
order by 1
