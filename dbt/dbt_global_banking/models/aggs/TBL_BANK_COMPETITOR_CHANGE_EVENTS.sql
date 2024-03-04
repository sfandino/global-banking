-- TBL_BANK_COMPETITOR_CHANGE_EVENTS Creation Script
-- This script tracks the changes in the key competitor bank for each customer over time.
-- It identifies any change in the top average bank as of the latest data ingestion and
-- records the event with a timestamp. The changes are stored in the 
-- TBL_BANK_COMPETITOR_CHANGE_EVENTS table, it provides insights into customer behavior and
-- bank competitiveness over time.

{{ config(materialized='table', 
   partition_by={'field': 'CHANGE_TIMESTAMP', 'data_type': 'timestamp'}) }}


WITH RankedBanks AS (
  SELECT
    HASHED_REAL_CUST_ID,
    TOP_AVG_BANK,
    INGESTION_TIMESTAMP,
    LAG(TOP_AVG_BANK) OVER(PARTITION BY HASHED_REAL_CUST_ID ORDER BY INGESTION_TIMESTAMP) AS PREV_TOP_BANK_CODE
  FROM {{ ref('TBL_KEY_COMPETITOR_BANK_CUSTOMER') }} 
)

SELECT
  HASHED_REAL_CUST_ID,
  PREV_TOP_BANK_CODE AS OLD_BANK_CODE,
  TOP_AVG_BANK AS NEW_BANK_CODE,
  TIMESTAMP(INGESTION_TIMESTAMP) AS CHANGE_TIMESTAMP
FROM RankedBanks
WHERE TOP_AVG_BANK != PREV_TOP_BANK_CODE
AND PREV_TOP_BANK_CODE IS NOT NULL