# Data Quality & Testing

## Data Quality Tests
- **Schema Validation**: Check column types, constraints, null values
- **Data Freshness**: Monitor data arrival times, alert on delays
- **Volume Checks**: Detect anomalous record counts (too high/low)
- **Referential Integrity**: Validate foreign key relationships

## Pipeline Testing
- **Unit Tests**: Test individual transformation functions
- **Integration Tests**: End-to-end pipeline validation
- **Data Lineage Tests**: Verify source-to-target mappings
- **Performance Tests**: Monitor processing times, resource usage

## Quality Metrics
- **Completeness**: Percentage of non-null required fields
- **Accuracy**: Data matches expected patterns/ranges
- **Consistency**: Cross-table validation, duplicate detection
- **Timeliness**: Data arrives within SLA windows