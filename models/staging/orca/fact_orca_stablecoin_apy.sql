{{ config(materialized="table") }}

with latest_per_group as (
  select
    name,
    fees,
    max(extraction_timestamp) as max_timestamp
  from {{ ref("fact_orca_apy") }}
  group by name, fees
),
latest_apy as (
  select
    f.extraction_timestamp as timestamp,
    f.id,
    concat(f.name, ' (', f.fees, '%)') as name,
    f.apy,
    f.tvl,
    f.symbol,
    f.protocol,
    f.type
  from {{ ref("fact_orca_apy") }} f
  join latest_per_group l
    on f.name = l.name
   and f.fees = l.fees
   and f.extraction_timestamp = l.max_timestamp
)

select
  l.timestamp
  , l.id
  , l.name
  , l.apy
  , l.tvl
  , l.symbol
  , l.protocol
  , l.type
  , p.link
from latest_apy l
join {{ ref("orca_stablecoin_pool_ids") }} p
  on l.id = p.id
