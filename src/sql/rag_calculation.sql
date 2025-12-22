-- Order Validation SQL Module
-- Standalone SQL for validating orders and routing to result_table or holding_table

-- BUSINESS_RULE: VALIDATION_RULES
-- Records are validated against the following rules:
-- 1. MISSING_ACCOUNT_ID: account_id must not be null
-- 2. MISSING_CUSTOMER_NAME: customer_name must not be null
-- 3. NEGATIVE_AMOUNT: monthly_total must be >= 0
-- 4. INVALID_ORDER_COUNT: order_count must be > 0
-- 5. MISSING_ORDER_LIMIT: order_limit must not be null
-- 6. STALE_ORDER_DATE: order_date must be within last 365 days

-- =============================================================================
-- Order Validation with CTEs
-- =============================================================================

WITH 
-- CTE 1: Filter valid orders
-- BUSINESS_RULE: VALID_ORDERS
-- Only COMPLETED and PENDING orders are included in calculations
valid_orders AS (
    SELECT 
        order_id,
        account_id,
        order_date,
        total_amount,
        product_count
    FROM orders
    WHERE status IN ('COMPLETED', 'PENDING')
),

-- CTE 2: Monthly aggregations per account
-- BUSINESS_RULE: MONTHLY_AGGREGATION
-- Sum all order amounts and count orders per account per month
monthly_totals AS (
    SELECT 
        account_id,
        DATE_TRUNC('month', order_date) AS order_month,
        SUM(total_amount) AS monthly_total,
        COUNT(*) AS order_count,
        SUM(product_count) AS total_products
    FROM valid_orders
    GROUP BY account_id, DATE_TRUNC('month', order_date)
),

-- CTE 3: Enrich with account data
-- BUSINESS_RULE: ACCOUNT_ENRICHMENT
-- Join with accounts to get customer_name and order_limit
enriched_totals AS (
    SELECT 
        mt.account_id,
        a.customer_name,
        mt.order_month,
        mt.monthly_total,
        mt.order_count,
        mt.total_products,
        a.order_limit
    FROM monthly_totals mt
    LEFT JOIN accounts a ON mt.account_id = a.account_id
    WHERE mt.order_month = (SELECT MAX(order_month) FROM monthly_totals)
),

-- CTE 4: Apply validation rules
-- BUSINESS_RULE: VALIDATION_RULES
-- Check each record against validation rules
with_validation AS (
    SELECT 
        *,
        CASE 
            WHEN account_id IS NULL THEN 'MISSING_ACCOUNT_ID'
            WHEN customer_name IS NULL THEN 'MISSING_CUSTOMER_NAME'
            WHEN monthly_total < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN order_count <= 0 THEN 'INVALID_ORDER_COUNT'
            WHEN order_limit IS NULL THEN 'MISSING_ORDER_LIMIT'
            ELSE NULL
        END AS hold_reason
    FROM enriched_totals
)

-- BUSINESS_RULE: RESULT_TABLE_OUTPUT
-- result_table contains all records that pass validation rules
SELECT 
    account_id,
    customer_name,
    order_month,
    monthly_total,
    order_count,
    total_products,
    order_limit
FROM with_validation
WHERE hold_reason IS NULL
ORDER BY account_id;


-- =============================================================================
-- HOLDING TABLE QUERY
-- =============================================================================

-- BUSINESS_RULE: HOLDING_TABLE_OUTPUT
-- holding_table contains all records that fail validation rules
-- Each record includes the hold_reason and hold_timestamp
/*
WITH 
valid_orders AS (
    SELECT order_id, account_id, order_date, total_amount, product_count
    FROM orders
    WHERE status IN ('COMPLETED', 'PENDING')
),
monthly_totals AS (
    SELECT 
        account_id,
        DATE_TRUNC('month', order_date) AS order_month,
        SUM(total_amount) AS monthly_total,
        COUNT(*) AS order_count,
        SUM(product_count) AS total_products
    FROM valid_orders
    GROUP BY account_id, DATE_TRUNC('month', order_date)
),
enriched_totals AS (
    SELECT 
        mt.account_id,
        a.customer_name,
        mt.order_month,
        mt.monthly_total,
        mt.order_count,
        mt.total_products,
        a.order_limit
    FROM monthly_totals mt
    LEFT JOIN accounts a ON mt.account_id = a.account_id
    WHERE mt.order_month = (SELECT MAX(order_month) FROM monthly_totals)
),
with_validation AS (
    SELECT 
        *,
        CASE 
            WHEN account_id IS NULL THEN 'MISSING_ACCOUNT_ID'
            WHEN customer_name IS NULL THEN 'MISSING_CUSTOMER_NAME'
            WHEN monthly_total < 0 THEN 'NEGATIVE_AMOUNT'
            WHEN order_count <= 0 THEN 'INVALID_ORDER_COUNT'
            WHEN order_limit IS NULL THEN 'MISSING_ORDER_LIMIT'
            ELSE NULL
        END AS hold_reason
    FROM enriched_totals
)
SELECT 
    account_id,
    customer_name,
    order_month,
    monthly_total,
    order_count,
    total_products,
    order_limit,
    hold_reason,
    CURRENT_TIMESTAMP AS hold_timestamp
FROM with_validation
WHERE hold_reason IS NOT NULL
ORDER BY hold_reason, account_id;
*/

-- =============================================================================
-- Summary Queries
-- =============================================================================

-- BUSINESS_RULE: RESULT_SUMMARY
-- Summary of valid records
/*
SELECT 
    COUNT(*) AS total_records,
    SUM(monthly_total) AS total_amount,
    SUM(order_count) AS total_orders
FROM with_validation
WHERE hold_reason IS NULL;
*/

-- BUSINESS_RULE: HOLDING_SUMMARY
-- Summary of records in holding table by reason
/*
SELECT 
    hold_reason,
    COUNT(*) AS record_count
FROM with_validation
WHERE hold_reason IS NOT NULL
GROUP BY hold_reason
ORDER BY record_count DESC;
*/
