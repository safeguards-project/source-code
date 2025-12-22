-- Customer Orders Pipeline - SQL Implementation
-- This SQL script implements the customer orders processing and validation logic
-- Outputs: result_table (valid records) and holding_table (invalid records)

-- BUSINESS_RULE: ORDER_FILTERING
-- Only include orders with status 'COMPLETED' or 'PENDING'
-- Exclude 'CANCELLED' orders from all calculations

-- BUSINESS_RULE: TRANSACTION_FILTERING  
-- Only include transactions with status 'SUCCESS'
-- Failed or pending transactions are excluded from payment calculations

-- Step 1: Create filtered orders view
-- BUSINESS_RULE: VALID_ORDERS
-- Valid orders are those with COMPLETED or PENDING status
CREATE OR REPLACE VIEW valid_orders AS
SELECT 
    order_id,
    account_id,
    order_date,
    total_amount,
    product_count,
    status
FROM orders
WHERE status IN ('COMPLETED', 'PENDING');

-- Step 2: Create filtered transactions view
-- BUSINESS_RULE: SUCCESSFUL_TRANSACTIONS
-- Only successful transactions count towards payment totals
CREATE OR REPLACE VIEW successful_transactions AS
SELECT
    transaction_id,
    order_id,
    amount,
    transaction_date,
    status
FROM transactions
WHERE status = 'SUCCESS';

-- Step 3: Join orders with transaction summaries
-- BUSINESS_RULE: ORDER_TRANSACTION_JOIN
-- Each order is enriched with its total paid amount and transaction count
CREATE OR REPLACE VIEW orders_with_payments AS
SELECT 
    o.order_id,
    o.account_id,
    o.order_date,
    o.total_amount,
    o.product_count,
    COALESCE(t.total_paid, 0) AS total_paid,
    COALESCE(t.transaction_count, 0) AS transaction_count
FROM valid_orders o
LEFT JOIN (
    SELECT 
        order_id,
        SUM(amount) AS total_paid,
        COUNT(transaction_id) AS transaction_count
    FROM successful_transactions
    GROUP BY order_id
) t ON o.order_id = t.order_id;

-- Step 4: Calculate monthly aggregations per account
-- BUSINESS_RULE: MONTHLY_AGGREGATION
-- Aggregate all orders by account and month to calculate:
-- - Total order amount
-- - Number of orders
-- - Total products ordered
CREATE OR REPLACE VIEW monthly_account_totals AS
SELECT 
    account_id,
    DATE_TRUNC('month', order_date) AS order_month,
    SUM(total_amount) AS monthly_total,
    COUNT(order_id) AS order_count,
    SUM(product_count) AS total_products
FROM orders_with_payments
GROUP BY account_id, DATE_TRUNC('month', order_date);

-- Step 5: Enrich with account data
-- BUSINESS_RULE: ACCOUNT_ENRICHMENT
-- Join monthly totals with account information
CREATE OR REPLACE VIEW enriched_monthly_totals AS
SELECT 
    mat.account_id,
    a.customer_name,
    mat.order_month,
    mat.monthly_total,
    mat.order_count,
    mat.total_products,
    a.order_limit
FROM monthly_account_totals mat
LEFT JOIN accounts a ON mat.account_id = a.account_id
WHERE mat.order_month = (SELECT MAX(order_month) FROM monthly_account_totals);

-- Step 6: Apply validation rules
-- BUSINESS_RULE: VALIDATION_RULES
-- Records are validated against multiple rules:
-- 1. MISSING_ACCOUNT_ID: account_id must not be null
-- 2. MISSING_CUSTOMER_NAME: customer_name must not be null
-- 3. NEGATIVE_AMOUNT: monthly_total must be >= 0
-- 4. INVALID_ORDER_COUNT: order_count must be > 0
-- 5. MISSING_ORDER_LIMIT: order_limit must not be null
CREATE OR REPLACE VIEW validated_orders AS
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
FROM enriched_monthly_totals;

-- =============================================================================
-- RESULT TABLE OUTPUT
-- =============================================================================

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
FROM validated_orders
WHERE hold_reason IS NULL
ORDER BY account_id;

-- =============================================================================
-- HOLDING TABLE OUTPUT
-- =============================================================================

-- BUSINESS_RULE: HOLDING_TABLE_OUTPUT
-- holding_table contains all records that fail validation rules
-- Each record includes the hold_reason and hold_timestamp
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
FROM validated_orders
WHERE hold_reason IS NOT NULL
ORDER BY hold_reason, account_id;

-- =============================================================================
-- SUMMARY QUERIES
-- =============================================================================

-- Result Summary Query
-- BUSINESS_RULE: RESULT_SUMMARY
-- Aggregate metrics for valid records
SELECT 
    COUNT(*) AS total_records,
    SUM(monthly_total) AS total_amount,
    SUM(order_count) AS total_orders
FROM validated_orders
WHERE hold_reason IS NULL;

-- Holding Summary Query
-- BUSINESS_RULE: HOLDING_SUMMARY
-- Summary of records in holding table by reason
SELECT 
    hold_reason,
    COUNT(*) AS record_count
FROM validated_orders
WHERE hold_reason IS NOT NULL
GROUP BY hold_reason
ORDER BY record_count DESC;
