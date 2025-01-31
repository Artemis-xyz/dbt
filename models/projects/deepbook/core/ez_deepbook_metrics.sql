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
    deepbook_tvl.date,
    'Defillama' as source,
    deepbook_tvl.tvl,
    deepbook_market_data.price,
    deepbook_market_data.market_cap,
    deepbook_market_data.fdmc,
    deepbook_market_data.token_turnover_circulating,
    deepbook_market_data.token_turnover_fdv,
    deepbook_market_data.token_volume
from deepbook_tvl
left join deepbook_market_data using (date)
where deepbook_tvl.date < to_date(sysdate())