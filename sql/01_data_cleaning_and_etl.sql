-- step 1 CREATE SCHEMA

CREATE SCHEMA kds_challenge;

-- step 2 CREATE TABLE

DROP TABLE IF EXISTS kds_transactions_raw;

CREATE TABLE kds_transactions_raw (
    transaction_date VARCHAR(20),
    client INT,
    amount_raw VARCHAR(20)
);

select * from kds_transactions_raw limit 10;

-- step 3: ETL & data cleaning

-- creating a temp table to avoid mutating original source data
DROP TABLE IF EXISTS kds_transactions_cleaned;

CREATE TABLE kds_transactions_cleaned AS
SELECT 
     client AS customer
    ,to_date(transaction_date, 'DD.MM.YYYY') AS t_date
    -- fixing decimal separators and casting to proper numeric type
    ,REPLACE(amount_raw, ',', '.')::DECIMAL(10,2) AS amount
FROM kds_challenge.kds_transactions_raw;

-- quality check
SELECT * 
FROM kds_transactions_cleaned
ORDER BY customer, t_date 
LIMIT 100;
