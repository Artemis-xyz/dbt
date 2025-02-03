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
    cetus_tvl.date,
    'Defillama' as source,
    cetus_tvl.tvl,
    cetus_market_data.price,
    cetus_market_data.market_cap,
    cetus_market_data.fdmc,
    cetus_market_data.token_turnover_circulating,
    cetus_market_data.token_turnover_fdv,
    cetus_market_data.token_volume
from cetus_tvl
left join cetus_market_data using (date)
where cetus_tvl.date < to_date(sysdate())