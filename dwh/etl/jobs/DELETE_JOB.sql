-- "Deletion" Job for Customer Data
-- This job performs the anonymization of customer data in the TBL_OB_ACCOUNT table.
-- It updates the REAL_CUST_ID and REAL_ACNO fields with non-identifiable information
-- when a customer withdraws their consent. The process ensures that the customer's
-- data cannot be traced back or matched with previous SHA256 hashed fields, maintaining
-- privacy and compliance with data protection regulations.

UPDATE `global-tech-ai.global_banking_protected.TBL_OB_ACCOUNT` ob
SET 
  ob.REAL_CUST_ID = -1,
  ob.REAL_ACNO = "NO_CONSENT"
FROM `global-tech-ai.global_banking_protected.TBL_CUSTOMER_CONSENT` cc
WHERE 
  ob.REAL_CUST_ID = cc.REAL_CUST_ID
  AND cc.CONSENT = FALSE
  AND EXISTS (
    SELECT 1
    FROM `global-tech-ai.global_banking_protected.TBL_CUSTOMER_CONSENT`
    WHERE REAL_CUST_ID = ob.REAL_CUST_ID
      AND CONSENT = FALSE
  );