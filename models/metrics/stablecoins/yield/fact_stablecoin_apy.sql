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
    , chain
    , link
    , tvl_score
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
    , chain
    , link
    , tvl_score
from {{ ref("fact_kamino_stablecoin_apy") }}
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
    , chain
    , link
    , tvl_score
from {{ ref("fact_save_stablecoin_apy") }}
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
    , chain
    , link
    , tvl_score
from {{ ref("fact_orca_stablecoin_apy") }}
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
    , chain
    , link
    , tvl_score
from {{ ref("fact_drift_stablecoin_apy") }}
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
    , chain
    , link
from {{ ref("fact_vaults_fyi_stablecoin_apy") }}