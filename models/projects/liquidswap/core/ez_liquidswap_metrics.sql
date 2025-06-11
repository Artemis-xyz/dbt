{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUIDSWAP',
        database='LIQUIDSWAP',
        schema='core',
        alias='ez_metrics'
    )
}}

with liquidswap_tvl as (
    {{ get_defillama_protocol_tvl('liquidswap') }}
)

, liquidswap_market_data as (
    {{ get_coingecko_metrics('pontem-liquidswap') }}
)

select
    liquidswap_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , liquidswap_tvl.tvl

    -- Market Metrics
    , lmd.price
    , lmd.market_cap
    , lmd.fdmc
    , lmd.token_turnover_circulating
    , lmd.token_turnover_fdv
    , lmd.token_volume
from liquidswap_tvl
left join liquidswap_market_data lmd using (date)
where liquidswap_tvl.date < to_date(sysdate())