{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_susdf_apy") }}
),

flattened as (
  select
    value as pool,
    base.extraction_date
    from base,
  lateral flatten(input => parse_json(base.source_json))
),

base_tvl as (
  select
    source_json,
    extraction_date
  from {{ source("PROD_LANDING", "raw_susdf_tvl") }}
),

flattened_tvl as (
  select
    value as pool,
    base_tvl.extraction_date
    from base_tvl,
  lateral flatten(input => parse_json(base_tvl.source_json))
),

extracted as (
  select
    date,
    ratio,
    lag(ratio) over (order by date) as previous_ratio
  from (
    select
      pool:date::timestamp as date,
      pool:value::float as ratio,
      extraction_date,
      row_number() over (
        partition by pool:date::timestamp
        order by extraction_date desc
      ) as rnk
    from flattened
  )
  where rnk = 1
),

extracted_tvl as (
  select
    date,
    tvl
  from (
    select
      pool:date::timestamp as date,
      pool:value::float as tvl,
      extraction_date,
      row_number() over (
        partition by pool:date::timestamp
        order by extraction_date desc
      ) as rnk
    from flattened_tvl
  )
  where rnk = 1
),

joined as (
  select
    e.date,
    case
      when e.previous_ratio is null then 0
      else (power(e.ratio / e.previous_ratio, 365) - 1)
    end as apy,
    et.tvl
  from extracted e
  join extracted_tvl et on e.date = et.date
)

select
  'Staked USDf Vault' as name,
  apy,
  tvl,
  array_construct('USDf') as symbol,
  'USDf' as protocol,
  'Vault' as type,
  'ethereum' as chain,
  'https://app.falcon.finance/earn/classic' as link,
  date as extraction_timestamp
from joined