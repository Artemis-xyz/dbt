{{
    config(
      materialized="table",
      unique_key="date",
    )
}}

with
  flattened_data as (
    select
      value:"timestamp"::timestamp as date,
      value:"value"::number as block_rewards_native
    from {{ source("PROD_LANDING", "raw_ton_minted") }},
          lateral flatten(input => SOURCE_JSON)
  )

select
  date(date) as date,
  max(block_rewards_native) as block_rewards_native
from flattened_data
group by 1