-- Positive value validation for amount
-- Type: data_quality
-- Priority: high
-- Description: Ensure amount is positive
-- Expected: negative_values = 0

SELECT COUNT(*) as negative_values FROM int_order_items_summary WHERE amount < 0;

-- Assertion: negative_values = 0
-- If this query returns any rows (or count > 0), the check has FAILED
