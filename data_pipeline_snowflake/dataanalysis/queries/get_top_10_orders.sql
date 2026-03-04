-- get_top_10_orders.sql
-- Purpose: Retrieve the top 10 orders by total price from the staging orders table.

SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority
FROM {{ ref('stg_tpch_orders') }}
ORDER BY o_totalprice DESC
LIMIT 10;