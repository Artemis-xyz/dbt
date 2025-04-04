{{
    config(
        materialized='table',
        snowflake_warehouse='VELODROME',
        database='VELODROME',
        schema='core',
        alias='ez_metrics'
    )
}}

with velodrome_tvl as (
    {{ get_defillama_protocol_tvl('velodrome') }}
)
, velodrome_market_data as (
    {{ get_coingecko_metrics('velodrome-finance') }}
)

select
    velodrome_tvl.date,
    'Defillama' as source,

    -- Standardized Metrics
    velodrome_tvl.tvl,
    velodrome_market_data.price,
    velodrome_market_data.market_cap,
    velodrome_market_data.fdmc,
    velodrome_market_data.token_turnover_circulating,
    velodrome_market_data.token_turnover_fdv,
    velodrome_market_data.token_volume
from velodrome_tvl
left join velodrome_market_data using (date) 