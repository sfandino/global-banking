-- TBL_MONTHLY_AVG_BALANCES_BANK_CUSTOMERS Creation Script
-- This script calculates and stores the monthly average balance in HKD for each customer
-- at each bank. It aggregates balance data from TBL_BALANCES_HKD and computes the average
-- per month, per bank, per customer.

{{ config(materialized='table') }}

WITH consent_dates AS(
  SELECT 
    SHA256(CAST(REAL_CUST_ID AS STRING)) AS HASHED_REAL_CUST_ID,
    BANK_CODE,
    MIN(EVENT_TIME) AS FIRST_CONSENT_DATE
  FROM {{ source('raw_data', 'TBL_CUSTOMER_CONSENT') }}
  WHERE CONSENT = TRUE
  GROUP BY 1,2
)

SELECT
  cb.HASHED_REAL_CUST_ID,
  cb.BANK_CODE,
  FORMAT_DATE('%Y%m', cb.UPDATE_DTTM) AS YEAR_MONTH,
  ROUND(AVG(cb.BALANCE_HKD), 2) AS AVG_BALANCE_HKD
FROM {{ ref('TBL_BALANCES_HKD') }} cb
JOIN consent_dates cd 
  ON cb.HASHED_REAL_CUST_ID = cd.HASHED_REAL_CUST_ID 
  AND cb.BANK_CODE = cd.BANK_CODE
WHERE 
  TIMESTAMP(cb.UPDATE_DTTM) >= cd.FIRST_CONSENT_DATE
  AND EXTRACT(DAYOFWEEK FROM cb.UPDATE_DTTM) <> 1 -- Excluding Sundays
GROUP BY cb.HASHED_REAL_CUST_ID, cb.BANK_CODE, YEAR_MONTH