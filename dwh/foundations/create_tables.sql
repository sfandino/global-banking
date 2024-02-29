-- Creating the TBL_OB_ACCOUNT table
CREATE TABLE `global-tech-ai.global_banking_protected.TBL_OB_ACCOUNT` (
  BANK_CODE STRING NOT NULL,
  REAL_ACNO STRING NOT NULL,
  REAL_CUST_ID NUMERIC NOT NULL,
  BALANCE NUMERIC,
  CURRENCY STRING,
  UPDATE_DTTM TIMESTAMP
);

-- Creating the TBL_CUSTOMER_CONSENT table
CREATE TABLE `global-tech-ai.global_banking_protected.TBL_CUSTOMER_CONSENT` (
  REAL_CUST_ID NUMERIC NOT NULL,
  BANK_CODE STRING NOT NULL,
  CONSENT BOOLEAN,
  EVENT_TIME TIMESTAMP
);

-- Creating the TBL_FX_RATE table
CREATE TABLE `global-tech-ai.external_data.TBL_FX_RATE` (
  FROM_CURRENCY STRING NOT NULL,
  TO_CURRENCY STRING NOT NULL,
  EXCHANGE_RATE FLOAT64,
  ACCURACY INT64,
  TIMESTAMP TIMESTAMP
);

-- Creating the TBL_BANK_COMPETITOR_CHANGE_EVENTS table with partitioning
CREATE TABLE `global-tech-ai.global_banking.TBL_BANK_COMPETITOR_CHANGE_EVENTS` (
  HASHED_REAL_CUST_ID STRING NOT NULL,
  OLD_BANK_CODE STRING,
  NEW_BANK_CODE STRING,
  CHANGE_TIMESTAMP TIMESTAMP
)
PARTITION BY DATE(CHANGE_TIMESTAMP);

-- Creating the CONVERTED_BALANCES_HKD table
CREATE TABLE `global-tech-ai.global_banking.TBL_BALANCES_HKD` (
  BANK_CODE STRING,
  HASHED_REAL_CUST_ID STRING,
  HASHED_REAL_ACNO STRING,
  BALANCE_HKD FLOAT64,
  UPDATE_DTTM DATETIME,
);

-- Creating the TBL_MONTHLY_AVG_BALANCES_BANK_CUSTOMER table
CREATE TABLE `global-tech-ai.global_banking.TBL_MONTHLY_AVG_BALANCES_BANK_CUSTOMER` (
  HASHED_REAL_CUST_ID STRING,
  BANK_CODE STRING,
  YEAR_MONTH STRING, 
  AVG_BALANCE_HKD FLOAT64,
);

-- Creating the TBL_AVG_BALANCES_BANK_CUSTOMER table
CREATE TABLE `global-tech-ai.global_banking.TBL_AVG_BALANCES_BANK_CUSTOMER` (
  HASHED_REAL_CUST_ID STRING,
  BANK_CODE STRING,
  INGESTION_TIMESTAMP TIMESTAMP,
  AVG_BALANCE_HKD_OVERALL FLOAT64,
);

-- Creating the TBL_KEY_COMPETITOR_BANK_CUSTOMER table 
CREATE TABLE `global-tech-ai.global_banking.TBL_KEY_COMPETITOR_BANK_CUSTOMER` (
  HASHED_REAL_CUST_ID STRING,
  TOP_AVG_BANK STRING,
  INGESTION_TIMESTAMP TIMESTAMP,
  TOP_BANK_BALANCE FLOAT64,
  RANKING INT64,
);

-- Creating the TBL_FACT_CUSTOMER_BANK table with partitioning
CREATE TABLE `global-tech-ai.global_banking_data_marts.TBL_FACT_CUSTOMER_BANK` (
  HASHED_REAL_CUST_ID STRING NOT NULL,
  HASHED_REAL_ACNO STRING NOT NULL,
  BANK_CODE STRING NOT NULL,
  AVG_BALANCE_HKD NUMERIC,
  FIRST_CONSENT_DATE TIMESTAMP,
  EXIT_CONSENT_DATE TIMESTAMP,
  TOP_AVG_BANK STRING,
  UPDATE_DTTM TIMESTAMP
)
PARTITION BY DATE(UPDATE_DTTM);

-- Creating the TBL_DATA_READINESS table
CREATE TABLE `global-tech-ai.global_banking_ops.TBL_DATA_READINESS` (
  TABLE_NAME STRING NOT NULL,
  DATA_LOAD_DATE TIMESTAMP NOT NULL,
  RECORD_COUNT INT64,
  STATUS STRING NOT NULL
)