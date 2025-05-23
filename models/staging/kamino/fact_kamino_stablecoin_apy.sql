{{ config(
    materialized="table"
) }}

with latest_vaults as (
  select
    id
    , extraction_timestamp
  from {{ ref("fact_kamino_vaults_apy") }}
  qualify row_number() over (partition by id order by extraction_timestamp desc) = 1
),

latest_lending as (
  select
    id
    , extraction_timestamp
  from {{ ref("fact_kamino_lending_apy") }}
  qualify row_number() over (partition by id order by extraction_timestamp desc) = 1
)

select
    v.extraction_timestamp as timestamp
    , v.id
    , v.name
    , v.apy
    , v.tvl
    , v.symbol
    , v.protocol
    , v.type
    , v.chain
    , v.link
    , a.tvl_score
    , a.daily_avg_apy_l7d
from {{ ref("fact_kamino_vaults_apy") }} v
join latest_vaults lv
on v.id = lv.id
    and v.extraction_timestamp = lv.extraction_timestamp
join {{ ref("agg_kamino_stablecoin_apy") }} a
on v.id = a.id
union all
select
    l.extraction_timestamp as timestamp
    , l.id
    , l.name
    , l.apy
    , l.tvl
    , l.symbol
    , l.protocol
    , l.type
    , l.chain
    , l.link
    , a.tvl_score
    , a.daily_avg_apy_l7d
from {{ ref("fact_kamino_lending_apy") }} l
join latest_lending ll
on l.id = ll.id
   and l.extraction_timestamp = ll.extraction_timestamp
join {{ ref("agg_kamino_stablecoin_apy") }} a
on l.id = a.id
