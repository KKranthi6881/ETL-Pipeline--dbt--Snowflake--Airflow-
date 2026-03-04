# Data Engineering Standards

## Naming Conventions
- **Datasets**: snake_case (e.g., customer_transactions, product_metrics)
- **Pipelines**: verb_noun_frequency (e.g., extract_sales_daily, transform_events_realtime)
- **Tables**: domain_entity_type (e.g., finance_revenue_fact, marketing_campaign_dim)
- **Columns**: snake_case with prefixes (created_at, updated_at, is_active)

## Pipeline Architecture
- **ELT over ETL**: Extract → Load → Transform using modern data warehouses
- **Idempotency**: All pipelines must handle reruns safely
- **Incremental Processing**: Use watermarks, timestamps for efficient updates
- **Data Lineage**: Track data flow from source to consumption

## Code Standards
- **SQL**: Use CTEs, avoid nested subqueries, add comments for complex logic
- **Python**: Type hints, docstrings, max 80 chars, use dataclasses
- **Config**: Environment-based, no hardcoded credentials
- **Error Handling**: Fail fast, log with context, implement retries