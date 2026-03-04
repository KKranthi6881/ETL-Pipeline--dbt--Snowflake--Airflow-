# Data Security & Governance

## Data Classification
- **Public**: Marketing data, public APIs (no restrictions)
- **Internal**: Business metrics, aggregated analytics (employee access)
- **Confidential**: Customer PII, financial data (role-based access)
- **Restricted**: Payment info, health data (strict need-to-know)

## Access Controls
- **Data Warehouse**: Role-based access by domain (finance, marketing, product)
- **Production DBs**: Read-only replicas for analytics, no direct access
- **API Keys**: Rotate every 90 days, scope to specific datasets
- **Query Logs**: All data access logged with user, query, timestamp

## Privacy Compliance
- **Data Masking**: PII masked in non-prod environments
- **Right to Delete**: Automated customer data deletion workflows
- **Data Lineage**: Track PII flow from source to consumption
- **Consent Management**: Honor opt-outs in all data processing

## Security Standards
- **Encryption**: AES-256 at rest, TLS 1.3 in transit
- **Backups**: Encrypted, tested monthly, 7-year retention
- **Monitoring**: Alert on unusual data access patterns