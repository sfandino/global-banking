
select * from {{ source('raw_data_api', 'TBL_FX_RATE') }}
  