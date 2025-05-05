{{ config( materialized="table") }}

select
    timestamp
    , id
    , name
    , apy
    , tvl
    , symbol
    , protocol
    , type
from {{ ref("fact_raydium_stablecoin_apy") }}
union all
select
    timestamp
    , id
    , name
    , apy
    , tvl
    , symbol
    , protocol
    , type
from {{ ref("fact_kamino_stablecoin_apy") }}