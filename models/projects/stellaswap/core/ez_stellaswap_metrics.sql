{{
    config(
        materialized='table',
        snowflake_warehouse='STELLASWAP',
        database='STELLASWAP',
        schema='core',
        alias='ez_metrics'
    )
}}

with stellaswap_tvl as (
    {{ get_defillama_protocol_tvl('stellaswap') }}
)
, stellaswap_market_data as (
    {{ get_coingecko_metrics('stellaswap') }}
)

select
    stellaswap_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , stellaswap_market_data.price
    , stellaswap_market_data.token_volume
    , stellaswap_market_data.market_cap
    , stellaswap_market_data.fdmc

    , stellaswap_tvl.tvl

    , stellaswap_market_data.token_turnover_circulating
    , stellaswap_market_data.token_turnover_fdv
from stellaswap_tvl
left join stellaswap_market_data using (date)
where stellaswap_tvl.date < to_date(sysdate())