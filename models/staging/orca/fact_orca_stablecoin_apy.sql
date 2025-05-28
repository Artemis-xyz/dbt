{{ config(materialized="table") }}

with latest_per_group as (
  select
    name
    , fees
    , extraction_timestamp
  from {{ ref("fact_orca_apy") }}
  qualify row_number() over (partition by id order by extraction_timestamp desc) = 1
)

select
  f.extraction_timestamp as timestamp
  , f.id
  , concat(f.name, ' (', f.fees, '%)') as name
  , f.apy
  , f.tvl
  , f.symbol
  , f.protocol
  , f.type
  , f.chain
  , f.link
  , a.tvl_score
  , a.daily_avg_apy_l7d
from {{ ref("fact_orca_apy") }} f
join latest_per_group l
  on f.name = l.name
  and f.fees = l.fees
  and f.extraction_timestamp = l.extraction_timestamp
join {{ ref("agg_orca_stablecoin_apy") }} a
  on f.id = a.id
