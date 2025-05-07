{{
    config(
        materialized='table',
        snowflake_warehouse='CETUS',
        database='CETUS',
        schema='core',
        alias='ez_metrics'
    )
}}

with cetus_tvl as (
    {{ get_defillama_protocol_tvl('cetus') }}
)
, cetus_market_data as (
    {{ get_coingecko_metrics('cetus-protocol') }}
)

select
    cetus_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , cetus_tvl.tvl

    -- Market Metrics
    , cmd.price
    , cmd.market_cap
    , cmd.fdmc
    , cmd.token_turnover_circulating
    , cmd.token_turnover_fdv
    , cmd.token_volume
from cetus_tvl
left join cetus_market_data cmd using (date)
where cetus_tvl.date < to_date(sysdate())