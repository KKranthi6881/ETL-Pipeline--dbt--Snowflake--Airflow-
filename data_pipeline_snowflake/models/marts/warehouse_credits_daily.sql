-- Materialized daily summary for Snowflake warehouse credit usage
-- Optimized for clustering and micro-partition pruning

{{ config(
    materialized='table',
    cluster_by=['day', 'account_name']
) }}

SELECT
    DATE_TRUNC('day', start_time) AS day,
    account_name,
    SUM(credits_used) AS credits_used,
    SUM(credits_used_compute) AS credits_compute,
    SUM(credits_used_cloud_services) AS credits_cloud
FROM SNOWFLAKE.ORGANIZATION_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY day DESC;