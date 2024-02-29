-- TBL_AVG_BALANCES_BANK_CUSTOMER Creation Script
-- This script aggregates customer balance data to calculate the overall average balance
-- per customer for each bank across all months. The result is stored in the 
-- TBL_AVG_BALANCES_BANK_CUSTOMER table. It provides a holistic view of the average funds
-- customers maintain in their accounts at each bank, disregarding the monthly breakdown.

SELECT
  HASHED_REAL_CUST_ID,
  BANK_CODE,
  CURRENT_TIMESTAMP() as INGESTION_TIMESTAMP,
  AVG(AVG_BALANCE_HKD) AS AVG_BALANCE_HKD_OVERALL
FROM `global-tech-ai.global_banking.TBL_MONTHLY_AVG_BALANCES_BANK_CUSTOMER`
GROUP BY HASHED_REAL_CUST_ID, BANK_CODE