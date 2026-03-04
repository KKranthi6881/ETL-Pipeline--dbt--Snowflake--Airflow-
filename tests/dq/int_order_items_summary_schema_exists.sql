-- Table int_order_items_summary exists
-- Type: schema_validation
-- Priority: high
-- Description: Verify int_order_items_summary table exists
-- Expected: table_exists = 1

SELECT COUNT(*) as table_exists FROM information_schema.tables WHERE table_name = 'int_order_items_summary';

-- Assertion: table_exists = 1
-- If this query returns any rows (or count > 0), the check has FAILED
