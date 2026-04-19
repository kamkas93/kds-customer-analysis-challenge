--------------------------------------------------------------------------------
-- PROJECT: KajoDataSpace Customer Lifecycle Challenge
-- SCRIPT: 02_Cohort_Analysis_and_Segmentation
-- DATABASE: PostgreSQL
-- GOAL: Advanced ETL: Cohort creation, Price Segmentation & LTV Tiering
--------------------------------------------------------------------------------

-- 1. Create Final Analytical Table
-- -----------------------------------------------------------------------------
-- This table is the "Single Source of Truth" for the Power BI Dashboard.
-- It integrates transaction data with behavioral cohorting and LTV segmentation.

DROP TABLE IF EXISTS kds_final_data;

CREATE TABLE kds_final_data AS 
WITH ranked_transactions AS (
    SELECT
        customer AS customer_id,
        t_date AS transaction_date,
        amount,
        -- Sequence of purchases to distinguish Acquisition (New) from Retention (Returning)
        ROW_NUMBER() OVER(PARTITION BY customer ORDER BY t_date ASC, amount DESC) AS trans_rank,
        -- Global acquisition date for each customer
        MIN(t_date) OVER(PARTITION BY customer) AS first_transaction_date,
        -- Calculating Total Lifetime Spend per customer for Tiering
        SUM(amount) OVER(PARTITION BY customer) AS total_customer_spend
    FROM kds_transactions_cleaned 
)
SELECT 
    customer_id,
    first_transaction_date,
    DATE_TRUNC('month', first_transaction_date)::DATE AS cohort_month,
    transaction_date,
    DATE_TRUNC('month', transaction_date)::DATE AS transaction_month,
    
    -- MONTH INDEX: Calculating customer age in months (Crucial for Retention Matrix)
    (EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', transaction_date)::DATE, 
                           DATE_TRUNC('month', first_transaction_date)::DATE)) * 12 +
     EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', transaction_date)::DATE, 
                            DATE_TRUNC('month', first_transaction_date)::DATE)))::INT AS month_number,
    
    amount,
    
    -- CUSTOMER LIFECYCLE SEGMENT
    CASE 
        WHEN trans_rank = 1 THEN 'New'
        ELSE 'Returning'
    END AS customer_segment,
    
    /* 1. PRICE SEGMENTATION (Transaction Level):
       Identifies the product tier of each individual payment. 
    */
    CASE 
        WHEN amount >= 890 THEN 'Elite Annual'
        WHEN amount IN (199, 169, 99, 249, 89, 179, 198, 219) THEN 'Classic Monthly'
        WHEN amount < 89 
             OR (amount < 169 AND (amount % 10 != 0 OR amount::TEXT LIKE '%.%')) 
             THEN 'Trial Starter'
        WHEN amount >= 250 AND amount < 890 THEN 'Smart Saver'
        ELSE 'Basic Access'
    END AS price_segment,

    /* 2. ENTRY SEGMENT (Customer Level):
       Freezes the starting plan. Used to track cohort behavior regardless of later upgrades.
    */
    FIRST_VALUE(
        CASE 
            WHEN amount >= 890 THEN 'Elite Annual'
            WHEN amount IN (199, 169, 99, 249, 89, 179, 198, 219) THEN 'Classic Monthly'
            WHEN amount < 89 
                 OR (amount < 169 AND (amount % 10 != 0 OR amount::TEXT LIKE '%.%')) 
                 THEN 'Trial Starter'
            WHEN amount >= 250 AND amount < 890 THEN 'Smart Saver'
            ELSE 'Basic Access'
        END
    ) OVER(PARTITION BY customer_id ORDER BY transaction_date ASC) AS entry_segment,

    /* 3. CUSTOMER TIER (LTV Segmentation):
       Categorizes customers by total lifetime contribution.
    */
    CASE 
        WHEN total_customer_spend > 1500 THEN 'Gold (High Value)'
        WHEN total_customer_spend BETWEEN 500 AND 1500 THEN 'Silver (Medium Value)'
        ELSE 'Bronze (Low Value)'
    END AS customer_tier

FROM ranked_transactions;


-- 2. Business Intelligence Queries (Audit & Validation)
-- -----------------------------------------------------------------------------

-- Analyzing price frequency to detect promos and pricing updates
SELECT 
    amount, 
    COUNT(*) AS how_often, 
    MIN(transaction_date) AS first_seen, 
    MAX(transaction_date) AS last_seen
FROM kds_transactions_cleaned
GROUP BY amount
ORDER BY how_often DESC;

-- Segment Validation: Checking price distribution per segment
SELECT 
    price_segment, 
    MIN(amount) AS min_val, 
    MAX(amount) AS max_val, 
    ROUND(AVG(amount), 2) AS avg_val, 
    COUNT(DISTINCT amount) AS unique_prices
FROM kds_final_data
GROUP BY price_segment
ORDER BY avg_val DESC;


-- 3. Data Integrity & Quality Checks
-- -----------------------------------------------------------------------------

-- Verify row count consistency
SELECT 
    (SELECT COUNT(*) FROM kds_final_data) AS final_rows,
    (SELECT COUNT(*) FROM kds_transactions_cleaned) AS source_rows;

-- Ensure 'New' segment only exists in the acquisition month (Month 0)
SELECT DISTINCT customer_segment, month_number 
FROM kds_final_data 
WHERE customer_segment = 'New'
ORDER BY month_number;
