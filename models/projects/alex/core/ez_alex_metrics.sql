{{
    config(
        materialized='table',
        snowflake_warehouse='ALEX',
        database='ALEX',
        schema='core',
        alias='ez_metrics'
    )
}}

with alex_tvl as (
    {{ get_defillama_protocol_tvl('alex') }}
)
, alex_market_data as (
    {{ get_coingecko_metrics('alexgo') }}
)

select
    alex_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , alex_tvl.tvl

    -- Market Metrics
    , alex_market_data.price
    , alex_market_data.market_cap
    , alex_market_data.fdmc
    , alex_market_data.token_turnover_circulating
    , alex_market_data.token_turnover_fdv
    , alex_market_data.token_volume
from alex_tvl
left join alex_market_data using (date)
where alex_tvl.date < to_date(sysdate())