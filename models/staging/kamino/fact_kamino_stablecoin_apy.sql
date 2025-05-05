{{ config(
    materialized="table"
) }}

select
    v.extraction_timestamp as timestamp,
    v.id,
    v.name,
    v.apy,
    v.tvl,
    v.symbol,
    v.protocol,
    v.type,
    s.link
from {{ ref("fact_kamino_vaults_apy") }} v
join {{ ref("kamino_stablecoin_vault_ids") }} s
  on v.id = s.id
union all
select
    extraction_timestamp,
    id,
    name,
    apy,
    tvl,
    symbol,
    protocol,
    type,
    null as link,
from {{ ref("fact_kamino_lending_apy") }}
