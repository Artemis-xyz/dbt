{{
    config(
        materialized='table',
        snowflake_warehouse='DEEPBOOK',
        database='DEEPBOOK',
        schema='core',
        alias='ez_metrics'
    )
}}

with deepbook_tvl as (
    {{ get_defillama_protocol_tvl('deepbook') }}
)
, deepbook_market_data as (
    {{ get_coingecko_metrics('deep') }}
)

select
    deepbook_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , deepbook_tvl.tvl

    -- Market Metrics
    , dmd.price
    , dmd.market_cap
    , dmd.fdmc
    , dmd.token_turnover_circulating
    , dmd.token_turnover_fdv
    , dmd.token_volume
from deepbook_tvl
left join deepbook_market_data dmd using (date)
where deepbook_tvl.date < to_date(sysdate())