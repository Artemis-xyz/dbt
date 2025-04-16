with
  flattened_data as (
    select
      value:"timestamp"::timestamp as date,
      value:"value"::number as block_rewards_native
    from {{ source("PROD_LANDING", "raw_ton_minted") }},
         lateral flatten(input => SOURCE_JSON)
  )

select
  date_trunc('day', date) as date,
  block_rewards_native
from flattened_data
