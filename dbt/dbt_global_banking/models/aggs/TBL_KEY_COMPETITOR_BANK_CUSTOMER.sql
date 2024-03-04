-- TBL_KEY_COMPETITOR_BANK_CUSTOMER Creation Script
-- This script identifies the primary banking relationship for each customer by determining
-- the bank with the highest overall average balance. It ranks banks based on the overall
-- average balance for each customer, storing the ranking in the TBL_KEY_COMPETITOR_BANK_CUSTOMER table.
-- This data helps in recognizing the main competitor banks for customer assets.

{{ config(materialized='table') }}

SELECT
    HASHED_REAL_CUST_ID,
    BANK_CODE AS TOP_AVG_BANK,
    INGESTION_TIMESTAMP,
    AVG_BALANCE_HKD_OVERALL AS TOP_BANK_BALANCE,
    RANK() OVER(PARTITION BY HASHED_REAL_CUST_ID ORDER BY AVG_BALANCE_HKD_OVERALL DESC) AS RANKING
FROM {{ ref('TBL_AVG_BALANCES_BANK_CUSTOMER') }}