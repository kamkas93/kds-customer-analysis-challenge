-- step 1: initialize schema
CREATE SCHEMA IF NOT EXISTS kds_challenge;

-- step 2: landing table for raw data
DROP TABLE IF EXISTS kds_challenge.kds_transactions;

CREATE TABLE kds_challenge.kds_transactions (
    transaction_date VARCHAR(20),
    client INT,
    amount_raw VARCHAR(20) -- keeping as varchar to handle formatting issues
);

-- step 3: preview raw data after import
SELECT * FROM kds_challenge.kds_transactions LIMIT 10;

-- step 4: ETL & data cleaning
-- creating a temp table to avoid mutating original source data
DROP TABLE IF EXISTS t_kds_cleaned;

CREATE TEMPORARY TABLE t_kds_cleaned AS
SELECT 
     client AS customer
    ,to_date(transaction_date, 'DD.MM.YYYY') AS t_date
    -- fixing decimal separators and casting to proper numeric type
    ,REPLACE(amount_raw, ',', '.')::DECIMAL(10,2) AS amount
FROM kds_challenge.kds_transactions;

-- quality check
SELECT * FROM t_kds_cleaned ORDER BY customer, t_date LIMIT 10;
