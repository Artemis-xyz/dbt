{{ config(
    materialized="table"
) }}

with latest_per_group as (
  select
    id
    , chain
    , extraction_timestamp
  from {{ ref("fact_vaults_fyi_apy") }}
  qualify row_number() over (partition by id, chain order by extraction_timestamp desc) = 1
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
from  {{ ref("fact_vaults_fyi_apy") }} f
join latest_per_group l
    on lower(f.id) = lower(l.id)
    and lower(f.chain) = lower(l.chain)
join {{ ref("agg_vaults_fyi_stablecoin_apy") }} a
    on lower(f.id) = lower(a.id)
    and lower(f.chain) = lower(a.chain)
    and f.extraction_timestamp = a.extraction_timestamp