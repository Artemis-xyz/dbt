{{
    config(
        materialized='table',
        snowflake_warehouse='CELLANA',
        database='CELLANA',
        schema='core',
        alias='ez_metrics'
    )
}}

with cellana_tvl as (
    {{ get_defillama_protocol_tvl('cellana') }}
)
, cellana_market_data as (
    {{ get_coingecko_metrics('cellena-finance') }}
)

select
    cellana_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , cellana_tvl.tvl

    -- Market Metrics
    , cmd.price
    , cmd.market_cap
    , cmd.fdmc
    , cmd.token_turnover_circulating
    , cmd.token_turnover_fdv
    , cmd.token_volume
from cellana_tvl
left join cellana_market_data cmd using (date)
where cellana_tvl.date < to_date(sysdate())