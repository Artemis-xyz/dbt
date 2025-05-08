{{ config(
    materialized="table"
) }}

with latest_vaults as (
  select
    id,
    max(extraction_timestamp) as max_timestamp
  from {{ ref("fact_kamino_vaults_apy") }}
  group by id
),
latest_lending as (
  select
    id,
    max(extraction_timestamp) as max_timestamp
  from {{ ref("fact_kamino_lending_apy") }}
  group by id
)
select
    v.extraction_timestamp as timestamp,
    v.id,
    v.name,
    v.apy,
    v.tvl,
    v.symbol,
    v.protocol,
    v.type,
    v.chain,
    s.link
from {{ ref("fact_kamino_vaults_apy") }} v
join latest_vaults lv
on v.id = lv.id
    and v.extraction_timestamp = lv.max_timestamp
join {{ ref("kamino_stablecoin_vault_ids") }} s
on v.id = s.id
union all
select
    extraction_timestamp,
    l.id,
    name,
    apy,
    tvl,
    symbol,
    protocol,
    type,
    chain,
    link,
from {{ ref("fact_kamino_lending_apy") }} l
join latest_lending ll
on l.id = ll.id
   and l.extraction_timestamp = ll.max_timestamp
join {{ ref("kamino_stablecoin_lending_ids") }} s
on l.id = s.id
