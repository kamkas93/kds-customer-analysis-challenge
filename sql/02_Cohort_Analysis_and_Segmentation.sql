--------------------------------------------------------------------------------
-- PROJECT: KajoDataSpace Customer Lifecycle Challenge
-- SCRIPT: 02_Cohort_Analysis_and_Segmentation
-- DATABASE: PostgreSQL
-- GOAL: Advanced ETL process for Power BI: Cohort creation & Price Segmentation
--------------------------------------------------------------------------------

-- 1. Create Final Analytical Table
-- -----------------------------------------------------------------------------
-- This table serves as the primary data source for Power BI Dashboard.
-- It combines transactional data with calculated cohort indices and behavioral segments.

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
        MIN(t_date) OVER(PARTITION BY customer) AS first_transaction_date
    FROM kds_transactions_cleaned 
)
SELECT 
    customer_id,
    first_transaction_date,
    DATE_TRUNC('month', first_transaction_date)::DATE AS cohort_month,
    transaction_date,
    DATE_TRUNC('month', transaction_date)::DATE AS transaction_month,
    
    -- MONTH INDEX: Calculating the age of a customer in months (Key for Retention Matrix)
    (EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', transaction_date)::DATE, 
                           DATE_TRUNC('month', first_transaction_date)::DATE)) * 12 +
     EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', transaction_date)::DATE, 
                            DATE_TRUNC('month', first_transaction_date)::DATE)))::INT AS month_number,
    
    amount,
    
    -- CUSTOMER SEGMENT: Defines the lifecycle stage of a given transaction
    CASE 
        WHEN trans_rank = 1 THEN 'New'
        ELSE 'Returning'
    END AS customer_segment,
    
    /* PRICE SEGMENTATION (Transaction Level):
       Logic based on price point distribution analysis. 
       Identifies current product tier for each transaction.
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

    /* ENTRY SEGMENT (Customer Level - KEY FOR RETENTION):
       Freezes the segment of the FIRST transaction using FIRST_VALUE.
       Ensures retention lines in Dashboard don't break when a customer upgrades their plan.
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
    ) OVER(PARTITION BY customer_id ORDER BY transaction_date ASC) AS entry_segment

FROM ranked_transactions;


-- 2. Business Intelligence Queries (Audit & Exploratory)
-- -----------------------------------------------------------------------------

-- Frequency analysis to identify pricing trends and promo effectiveness
SELECT 
    amount, 
    COUNT(*) AS frequency, 
    MIN(transaction_date) AS first_seen, 
    MAX(transaction_date) AS last_seen
FROM kds_final_data
GROUP BY amount
ORDER BY frequency DESC;

-- Segment validation: Checking if logic correctly captures the price ranges
SELECT 
    price_segment, 
    MIN(amount) AS min_amount, 
    MAX(amount) AS max_amount, 
    ROUND(AVG(amount), 2) AS avg_amount, 
    COUNT(DISTINCT amount) AS unique_price_points
FROM kds_final_data
GROUP BY price_segment
ORDER BY avg_amount DESC;


-- 3. Data Integrity & Quality Checks
-- -----------------------------------------------------------------------------

-- Row count verification (should match source table)
SELECT 
    (SELECT COUNT(*) FROM kds_final_data) AS final_rows,
    (SELECT COUNT(*) FROM kds_transactions_cleaned) AS source_rows;

-- Sanity check: 'New' customers should strictly appear in month_number 0
SELECT DISTINCT customer_segment, month_number 
FROM kds_final_data 
WHERE customer_segment = 'New'
ORDER BY month_number;
