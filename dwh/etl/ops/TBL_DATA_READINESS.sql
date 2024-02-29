-- TBL_DATA_READINESS Table Loading Script
-- This script writes the TBL_DATA_READINESS table for tracking the readiness status
-- of various tables within the data warehouse after ETL processes. It records the table name,
-- the timestamp when the data load was completed, the count of records loaded, and the status
-- of the data load operation.


SELECT
  'TBL_FACT_CUSTOMER_BANK' AS TABLE_NAME,
  CURRENT_TIMESTAMP() AS DATA_LOAD_DATE,
  (SELECT COUNT(*) FROM `global-tech-ai.global_banking.TBL_FACT_CUSTOMER_BANK`) AS RECORD_COUNT, -- this is an example of the SQL - This should be templated and work dinamically
  'COMPLETED' AS STATUS;