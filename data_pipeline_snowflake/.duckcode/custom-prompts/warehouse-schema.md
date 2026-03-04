# Data Warehouse Schema

## Dimensional Modeling
- **Fact Tables**: Contain metrics/measures (sales_fact, user_events_fact)
- **Dimension Tables**: Contain descriptive attributes (customer_dim, product_dim)
- **Bridge Tables**: Handle many-to-many relationships
- **Slowly Changing Dimensions**: Use SCD Type 2 for historical tracking

## Naming Standards
- **Facts**: business_process_fact (e.g., sales_fact, web_events_fact)
- **Dimensions**: entity_dim (e.g., customer_dim, product_dim, date_dim)
- **Staging**: stg_source_table (e.g., stg_salesforce_accounts)
- **Marts**: domain_entity (e.g., finance_revenue, marketing_campaigns)

## Standard Columns
- **Surrogate Keys**: table_sk (bigint, auto-increment)
- **Natural Keys**: entity_id (from source system)
- **Audit**: created_at, updated_at, source_system, batch_id
- **SCD**: effective_from, effective_to, is_current

## Data Types
- **Metrics**: DECIMAL(18,4) for financial, BIGINT for counts
- **Dates**: DATE for calendar dates, TIMESTAMP for events
- **Text**: VARCHAR(255) for codes, TEXT for descriptions