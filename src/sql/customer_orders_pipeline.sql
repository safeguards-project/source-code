-- Customer Orders Pipeline - SQL Implementation
-- This SQL script implements the customer orders processing and RAG calculation logic

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

-- Step 5: Calculate month-over-month changes
-- BUSINESS_RULE: MOM_COMPARISON
-- Compare current month total against previous month total
-- Calculate percentage change: (current - previous) / previous * 100
CREATE OR REPLACE VIEW monthly_changes AS
SELECT 
    account_id,
    order_month,
    monthly_total AS current_month_total,
    LAG(monthly_total, 1) OVER (
        PARTITION BY account_id 
        ORDER BY order_month
    ) AS previous_month_total,
    order_count,
    total_products,
    CASE 
        WHEN LAG(monthly_total, 1) OVER (
            PARTITION BY account_id 
            ORDER BY order_month
        ) IS NULL THEN NULL
        WHEN LAG(monthly_total, 1) OVER (
            PARTITION BY account_id 
            ORDER BY order_month
        ) = 0 THEN NULL
        ELSE (
            (monthly_total - LAG(monthly_total, 1) OVER (
                PARTITION BY account_id 
                ORDER BY order_month
            )) / LAG(monthly_total, 1) OVER (
                PARTITION BY account_id 
                ORDER BY order_month
            )
        ) * 100
    END AS percentage_change
FROM monthly_account_totals;

-- Step 6: Determine RAG status
-- BUSINESS_RULE: RAG_STATUS_DETERMINATION
-- RAG thresholds:
-- - RED: percentage_change >= 50%
-- - AMBER: percentage_change >= 30% AND < 50%
-- - GREEN: percentage_change < 30% OR NULL (new customers)
CREATE OR REPLACE VIEW rag_status_view AS
SELECT 
    mc.account_id,
    a.customer_name,
    mc.order_month,
    mc.current_month_total,
    mc.previous_month_total,
    mc.percentage_change,
    mc.order_count,
    a.order_limit,
    CASE 
        WHEN mc.percentage_change IS NULL THEN 'GREEN'
        WHEN mc.percentage_change >= 50 THEN 'RED'
        WHEN mc.percentage_change >= 30 THEN 'AMBER'
        ELSE 'GREEN'
    END AS rag_status,
    -- BUSINESS_RULE: ORDER_LIMIT_CHECK
    -- Flag accounts that exceed their monthly order limit
    CASE 
        WHEN mc.order_count > a.order_limit THEN 'YES'
        ELSE 'NO'
    END AS limit_exceeded
FROM monthly_changes mc
JOIN accounts a ON mc.account_id = a.account_id;

-- Step 7: Final RAG Analysis Query
-- BUSINESS_RULE: RAG_ANALYSIS_FINAL
-- Returns the complete RAG analysis for the most recent month
-- Includes all risk indicators and customer information
SELECT 
    account_id,
    customer_name,
    current_month_total,
    previous_month_total,
    ROUND(percentage_change, 2) AS percentage_change,
    order_count,
    order_limit,
    rag_status,
    limit_exceeded
FROM rag_status_view
WHERE order_month = (SELECT MAX(order_month) FROM rag_status_view)
ORDER BY 
    CASE rag_status 
        WHEN 'RED' THEN 1 
        WHEN 'AMBER' THEN 2 
        ELSE 3 
    END,
    percentage_change DESC NULLS LAST;

-- Risk Summary Query
-- BUSINESS_RULE: RISK_SUMMARY
-- Aggregate counts for monitoring and reporting
SELECT 
    COUNT(*) AS total_accounts,
    SUM(CASE WHEN rag_status = 'RED' THEN 1 ELSE 0 END) AS red_count,
    SUM(CASE WHEN rag_status = 'AMBER' THEN 1 ELSE 0 END) AS amber_count,
    SUM(CASE WHEN rag_status = 'GREEN' THEN 1 ELSE 0 END) AS green_count,
    SUM(CASE WHEN limit_exceeded = 'YES' THEN 1 ELSE 0 END) AS limit_exceeded_count
FROM rag_status_view
WHERE order_month = (SELECT MAX(order_month) FROM rag_status_view);
