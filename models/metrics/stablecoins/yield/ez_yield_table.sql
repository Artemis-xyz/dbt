{{ config( materialized="table") }}

select
    timestamp
    , name
    , apy * 100 as apy
    , tvl
    , symbol
    , protocol
    , type
    , chain
    , link
    , tvl_score as risk_score
from {{ ref("fact_stablecoin_apy") }}
union all
select
    timestamp
    , name
    , apy * 100 as apy
    , tvl
    , symbol
    , protocol
    , type
    , chain
    , link
    , null as risk_score
from {{ ref("fact_fedfunds_rates") }}
qualify row_number() over (partition by name, protocol, link order by timestamp desc) = 1