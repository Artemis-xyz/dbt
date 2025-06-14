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
    , daily_avg_apy_l7d
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
    , daily_avg_apy_l7d
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
    , daily_avg_apy_l7d
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
    , daily_avg_apy_l7d
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
    , daily_avg_apy_l7d
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
    , tvl_score
    , daily_avg_apy_l7d
from {{ ref("fact_vaults_fyi_stablecoin_apy") }}
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
    , daily_avg_apy_l7d
from {{ ref("fact_pendle_stablecoin_apy") }}
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
    , daily_avg_apy_l7d
from {{ ref("fact_morpho_stablecoin_apy") }}
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
    , daily_avg_apy_l7d
from {{ ref("fact_susdf_stablecoin_apy") }}