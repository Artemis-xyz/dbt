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
    cellana_tvl.date,
    'Defillama' as source,
    cellana_tvl.tvl,
    cellana_market_data.price,
    cellana_market_data.market_cap,
    cellana_market_data.fdmc,
    cellana_market_data.token_turnover_circulating,
    cellana_market_data.token_turnover_fdv,
    cellana_market_data.token_volume
from cellana_tvl
left join cellana_market_data using (date)
where cellana_tvl.date < to_date(sysdate())