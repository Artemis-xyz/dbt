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

joined as (
  select
    e.date,
    case
      when e.previous_ratio is null then 0
      else (power(e.ratio / e.previous_ratio, 365*24) - 1)
    end as apy,
    coalesce(et.tvl, latest_tvl.tvl) as tvl
  from extracted e
  left join {{ ref("fact_susdf_tvl") }} et
    on to_date(e.date) = et.date
  left join (
    select tvl
    from {{ ref("fact_susdf_tvl") }}
    qualify row_number() over (order by date desc) = 1
  ) latest_tvl on true
)

select
  'Staked USDf Vault' as name,
  apy,
  tvl,
  array_construct('USDFALCON') as symbol,
  'falcon_finance' as protocol,
  'Vault' as type,
  'ethereum' as chain,
  'https://app.falcon.finance/earn/classic' as link,
  date as extraction_timestamp
from joined