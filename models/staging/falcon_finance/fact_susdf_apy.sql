{{ config(materialized="table") }}

with base as (
  select
    source_json,
    extraction_date,
    source_url
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
        extraction_date,
        source_url
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
    pool:date::timestamp as date,
    pool:value::float as ratio,
    lag(pool:value::float) over (order by pool:date::timestamp) as previous_ratio,
  from flattened
),

extracted_tvl as (
  select
    pool:date::timestamp as date,
    pool:value::float as tvl,
  from flattened_tvl
),

joined as (
  select
    e.date,
    case
      when e.previous_ratio is null then 0
      else (power(e.ratio / e.previous_ratio, 365) - 1)
    end as apy,
    et.tvl,
    row_number() over (partition by e.date order by e.date desc) as rnk
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
where rnk = 1