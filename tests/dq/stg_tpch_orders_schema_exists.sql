-- Table stg_tpch_orders exists
-- Type: schema_validation
-- Priority: high
-- Description: Verify stg_tpch_orders table exists
-- Expected: table_exists = 1

SELECT COUNT(*) as table_exists FROM information_schema.tables WHERE table_name = 'stg_tpch_orders';

-- Assertion: table_exists = 1
-- If this query returns any rows (or count > 0), the check has FAILED
