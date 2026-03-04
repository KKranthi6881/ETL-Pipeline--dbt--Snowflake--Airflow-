# Data API Standards

## Data API Patterns
- **Streaming**: /api/v1/streams/dataset/events for real-time data
- **Batch**: /api/v1/datasets/name/export for bulk data access
- **Query**: /api/v1/query with SQL/GraphQL for ad-hoc analysis
- **Metrics**: /api/v1/metrics/domain for aggregated KPIs

## Response Formats
- **Streaming**: JSONL (newline-delimited JSON) for events
- **Batch**: Parquet/CSV with compression for large datasets
- **Pagination**: cursor-based for time-series, offset for small datasets
- **Metadata**: Include schema, lineage, freshness info

## Authentication
- **Service Accounts**: For pipeline-to-pipeline communication
- **User Tokens**: JWT with data access scopes (read, write, admin)
- **Rate Limits**: 10k requests/hour for analytics, 1k/hour for exports
- **Data Governance**: Log all data access for compliance

## Error Handling
- **Schema Validation**: Return detailed field-level errors
- **Data Quality**: Include data freshness and quality scores
- **Timeouts**: 30s for queries, 5min for exports, streaming keeps alive