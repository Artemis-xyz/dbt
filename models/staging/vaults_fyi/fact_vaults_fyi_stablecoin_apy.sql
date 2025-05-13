{{ config(
    materialized="table"
) }}

select
    f.extraction_timestamp as timestamp
    , f.id
    , f.name
    , f.apy
    , f.tvl
    , array_construct(v.symbol) as symbol
    , f.protocol
    , f.type
    , f.chain
    , f.link
from  {{ ref("fact_vaults_fyi_apy") }} f
join {{ ref("vaults_fyi_stablecoin_pool_ids") }} v
    on f.id = v.id