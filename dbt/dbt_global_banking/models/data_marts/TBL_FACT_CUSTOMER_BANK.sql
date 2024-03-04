-- TBL_FACT_CUSTOMER_BANK Creation Script
-- This script assembles the TBL_FACT_CUSTOMER_BANK table, which serves as a fact table
-- in the data mart. It includes the latest average balance in HKD, first and last consent dates,
-- and the top average bank for each customer. This table is designed to support analytical queries
-- regarding customer balances and their banking preferences over time.

{{ config(
    materialized='table',
    partition_by={
        "field": "UPDATE_DTTM",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

WITH consent_dates AS (
  SELECT
    REAL_CUST_ID,
    BANK_CODE,
    MIN(CASE WHEN CONSENT = TRUE THEN EVENT_TIME END) AS FIRST_CONSENT_DATE,
    MAX(CASE WHEN CONSENT = FALSE THEN EVENT_TIME END) AS EXIT_CONSENT_DATE
  FROM {{ source('raw_data', 'TBL_CUSTOMER_CONSENT') }}
  GROUP BY REAL_CUST_ID, BANK_CODE
)

SELECT 
  TO_BASE64(SHA256(CAST(ob.REAL_CUST_ID AS STRING))) AS HASHED_REAL_CUST_ID,
  ob.BANK_CODE,
  AVG(ma.AVG_BALANCE_HKD) AS AVG_BALANCE_HKD, -- calculates the average over the monthly balances previously calculated 
  cd.FIRST_CONSENT_DATE,
  cd.EXIT_CONSENT_DATE,
  kcb.TOP_AVG_BANK,
  CURRENT_TIMESTAMP() AS UPDATE_DTTM
FROM {{ source('raw_data', 'TBL_OB_ACCOUNT') }} ob
JOIN consent_dates cd ON ob.REAL_CUST_ID = cd.REAL_CUST_ID AND ob.BANK_CODE = cd.BANK_CODE
JOIN {{ ref('TBL_MONTHLY_AVG_BALANCES_BANK_CUSTOMER') }} ma ON SHA256(CAST(ob.REAL_CUST_ID AS STRING)) = ma.HASHED_REAL_CUST_ID
JOIN {{ ref('TBL_KEY_COMPETITOR_BANK_CUSTOMER') }} kcb ON SHA256(CAST(ob.REAL_CUST_ID AS STRING)) = kcb.HASHED_REAL_CUST_ID
WHERE ob.UPDATE_DTTM >= cd.FIRST_CONSENT_DATE
  AND (cd.EXIT_CONSENT_DATE IS NULL OR ob.UPDATE_DTTM <= cd.EXIT_CONSENT_DATE)
  AND kcb.RANKING = 1
GROUP BY 1, 2, 4, 5, 6
