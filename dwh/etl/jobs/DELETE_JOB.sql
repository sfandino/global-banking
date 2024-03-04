-- "Deletion" Job for Customer Data
-- This job performs the anonymization of customer data in the TBL_OB_ACCOUNT table.
-- It updates the REAL_CUST_ID and REAL_ACNO fields with non-identifiable information
-- when a customer withdraws their consent. The process ensures that the customer's
-- data cannot be traced back or matched with previous SHA256 hashed fields, maintaining
-- privacy and compliance with data protection regulations.

MERGE INTO `global-tech-ai.global_banking_protected.TBL_OB_ACCOUNT_backup_copy2` AS accounts
USING (
    WITH RankedConsents AS (
      SELECT
        REAL_CUST_ID,
        BANK_CODE,
        CONSENT,
        EVENT_TIME,
        ROW_NUMBER() OVER (PARTITION BY REAL_CUST_ID, BANK_CODE ORDER BY EVENT_TIME DESC) as rn
      FROM `global-tech-ai.global_banking_protected.TBL_CUSTOMER_CONSENT`
    )
    SELECT
      REAL_CUST_ID,
      BANK_CODE
    FROM RankedConsents
    WHERE rn = 1 AND CONSENT = FALSE
) AS consents
ON accounts.REAL_CUST_ID = consents.REAL_CUST_ID AND accounts.BANK_CODE = consents.BANK_CODE
WHEN MATCHED THEN
  UPDATE SET accounts.REAL_CUST_ID = -1, accounts.REAL_ACNO = 'NO CONSENT'
