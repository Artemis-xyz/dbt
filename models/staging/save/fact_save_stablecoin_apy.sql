{{ config(
    materialized="table"
) }}

with latest as (
  select
    id
    , extraction_timestamp 
  from {{ ref("fact_save_apy") }}
  qualify row_number() over (partition by id order by extraction_timestamp desc) = 1
)

select
  f.extraction_timestamp as timestamp
  , f.id
  , f.name
  , f.apy
  , f.tvl
  , f.symbol
  , f.protocol
  , f.type
  , f.chain
  , f.link
  , a.tvl_score
  , a.daily_avg_apy_l7d
from {{ ref("fact_save_apy") }} f
join latest l
on f.id = l.id
  and f.extraction_timestamp = l.extraction_timestamp
join {{ ref("agg_save_stablecoin_apy") }} a
on f.id = a.id
