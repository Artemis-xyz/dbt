{{ config(
    materialized="table",
    unique_key=["id", "extraction_timestamp"]
) }}

select
    v.extraction_timestamp as timestamp,
    v.id,
    v.name,
    v.apy,
    v.tvl,
    v.symbol,
    v.protocol,
    v.type
from {{ ref("fact_kamino_vaults_apy") }} v
join {{ ref("fact_kamino_stablecoin_vault_ids") }} s
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
    type
from {{ ref("fact_kamino_lending_apy") }}
