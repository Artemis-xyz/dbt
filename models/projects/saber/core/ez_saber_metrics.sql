{{
    config(
        materialized='table',
        snowflake_warehouse='SABER',
        database='SABER',
        schema='core',
        alias='ez_metrics'
    )
}}

with saber_tvl as (
    {{ get_defillama_protocol_tvl('saber') }}
),
saber_market_data as (
    {{ get_coingecko_metrics('saber') }}
)

select
    saber_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , saber_tvl.tvl

    -- Market Metrics
    , smd.price
    , smd.market_cap
    , smd.fdmc
    , smd.token_turnover_circulating
    , smd.token_turnover_fdv
    , smd.token_volume
from saber_tvl
left join saber_market_data smd using (date)
where saber_tvl.date < to_date(sysdate())