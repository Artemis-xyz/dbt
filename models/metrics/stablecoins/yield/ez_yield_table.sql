{{ config( materialized="table") }}

select
    timestamp
    , name
    , apy * 100
    , tvl
    , symbol
    , protocol
    , type
    , link
from {{ ref("fact_stablecoin_apy") }}
union all
select
    timestamp
    , name
    , apy * 100
    , tvl
    , symbol
    , protocol
    , type
    , link
from {{ ref("fact_fedfunds_rates") }}