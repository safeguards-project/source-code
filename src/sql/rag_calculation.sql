-- RAG Calculation SQL Module
-- Standalone SQL for RAG (Red/Amber/Green) status calculation

-- BUSINESS_RULE: RAG_THRESHOLDS
-- The RAG system uses the following thresholds for month-over-month comparison:
-- - RED: Current month total is 50% or more higher than previous month
-- - AMBER: Current month total is 30-49% higher than previous month
-- - GREEN: Current month total is less than 30% higher than previous month

-- =============================================================================
-- RAG Calculation with CTEs for clarity
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
        COUNT(*) AS order_count
    FROM valid_orders
    GROUP BY account_id, DATE_TRUNC('month', order_date)
),

-- CTE 3: Add previous month data using window function
-- BUSINESS_RULE: MOM_COMPARISON
-- Use LAG to get previous month's total for comparison
with_previous_month AS (
    SELECT 
        account_id,
        order_month,
        monthly_total,
        order_count,
        LAG(monthly_total) OVER (
            PARTITION BY account_id 
            ORDER BY order_month
        ) AS previous_month_total
    FROM monthly_totals
),

-- CTE 4: Calculate percentage change
-- BUSINESS_RULE: PERCENTAGE_CALCULATION
-- Formula: ((current - previous) / previous) * 100
-- Handle NULL and zero previous month cases
with_percentage AS (
    SELECT 
        account_id,
        order_month,
        monthly_total AS current_month_total,
        previous_month_total,
        order_count,
        CASE 
            WHEN previous_month_total IS NULL THEN NULL
            WHEN previous_month_total = 0 THEN NULL
            ELSE ((monthly_total - previous_month_total) / previous_month_total) * 100
        END AS percentage_change
    FROM with_previous_month
),

-- CTE 5: Apply RAG status based on thresholds
-- BUSINESS_RULE: RAG_STATUS_DETERMINATION
-- RED >= 50%, AMBER >= 30% and < 50%, GREEN < 30% or new customer
with_rag_status AS (
    SELECT 
        wp.account_id,
        a.customer_name,
        wp.order_month,
        wp.current_month_total,
        wp.previous_month_total,
        wp.percentage_change,
        wp.order_count,
        a.order_limit,
        CASE 
            WHEN wp.percentage_change IS NULL THEN 'GREEN'
            WHEN wp.percentage_change >= 50.0 THEN 'RED'
            WHEN wp.percentage_change >= 30.0 THEN 'AMBER'
            ELSE 'GREEN'
        END AS rag_status,
        -- BUSINESS_RULE: ORDER_LIMIT_CHECK
        -- Check if monthly order count exceeds account limit
        CASE 
            WHEN wp.order_count > a.order_limit THEN 'YES'
            ELSE 'NO'
        END AS limit_exceeded
    FROM with_percentage wp
    INNER JOIN accounts a ON wp.account_id = a.account_id
)

-- Final output: RAG results for target month
-- BUSINESS_RULE: RAG_OUTPUT
-- Return sorted results with RED accounts first, then AMBER, then GREEN
SELECT 
    account_id,
    customer_name,
    current_month_total,
    previous_month_total,
    ROUND(percentage_change::numeric, 2) AS percentage_change,
    order_count,
    order_limit,
    rag_status,
    limit_exceeded
FROM with_rag_status
WHERE order_month = (SELECT MAX(order_month) FROM with_rag_status)
ORDER BY 
    CASE rag_status 
        WHEN 'RED' THEN 1 
        WHEN 'AMBER' THEN 2 
        WHEN 'GREEN' THEN 3 
    END,
    percentage_change DESC NULLS LAST;


-- =============================================================================
-- Alternative: Parameterized version for specific month analysis
-- =============================================================================

-- To analyze a specific month, replace the WHERE clause:
-- WHERE order_month = '2024-12-01'::date

-- =============================================================================
-- RAG Status Distribution Query
-- =============================================================================

-- BUSINESS_RULE: RAG_DISTRIBUTION
-- Summary query for dashboard/reporting showing distribution of RAG statuses

/*
SELECT 
    rag_status,
    COUNT(*) AS account_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    SUM(current_month_total) AS total_revenue,
    AVG(percentage_change) AS avg_change
FROM with_rag_status
WHERE order_month = (SELECT MAX(order_month) FROM with_rag_status)
GROUP BY rag_status
ORDER BY 
    CASE rag_status 
        WHEN 'RED' THEN 1 
        WHEN 'AMBER' THEN 2 
        WHEN 'GREEN' THEN 3 
    END;
*/
