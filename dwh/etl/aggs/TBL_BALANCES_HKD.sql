
- TBL_BALANCES_HKD Creation Script
-- This script generates the TBL_BALANCES_HKD table which stores the HKD equivalent
-- balances for customer accounts. The balances are converted using the most recent
-- exchange rates available. The table is a result of joining the account details
-- from TBL_OB_ACCOUNT with the exchange rates from TBL_FX_RATE, with a focus on
-- converting balances to Hong Kong Dollars (HKD). 

SELECT 
  a.BANK_CODE,
  SHA256(CAST(a.REAL_CUST_ID AS STRING)) AS HASHED_REAL_CUST_ID,
  SHA256(CAST(a.REAL_ACNO AS STRING)) AS HASHED_REAL_ACNO,
  a.BALANCE * IFNULL(r.EXCHANGE_RATE, 1) AS BALANCE_HKD,  -- Assuming a default exchange rate of 1 if not found, but if we have the backup methods on place named on the doc, it should not happen.
  a.UPDATE_DTTM
FROM `global-tech-ai.global_banking_protected.TBL_OB_ACCOUNT` a
LEFT JOIN `global-tech-ai.external_data.TBL_FX_RATE` r 
  ON a.CURRENCY = r.FROM_CURRENCY 
  AND r.TO_CURRENCY = 'HKD'
  AND a.UPDATE_DTTM = r.TIMESTAMP
