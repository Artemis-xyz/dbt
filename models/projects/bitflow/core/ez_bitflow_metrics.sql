{{
    config(
        materialized='table',
        snowflake_warehouse='BITFLOW',
        database='BITFLOW',
        schema='core',
        alias='ez_metrics'
    )
}}

with bitflow_tvl as (
    {{ get_defillama_protocol_tvl('bitflow') }}
)

, bitflow_market_data as (
    {{get_coingecko_metrics('bitflow')}}
)

select
    bitflow_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , bitflow_tvl.tvl

    -- Market Metrics
    , bmm.price
    , bmm.market_cap
    , bmm.fdmc
    , bmm.token_turnover_circulating
    , bmm.token_turnover_fdv
    , bmm.token_volume
from bitflow_tvl
left join bitflow_market_data bmm using (date)
where bitflow_tvl.date < to_date(sysdate())
