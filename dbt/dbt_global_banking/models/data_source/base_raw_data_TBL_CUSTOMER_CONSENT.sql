
select * from {{ source('raw_data', 'TBL_CUSTOMER_CONSENT') }}
  