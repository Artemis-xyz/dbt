{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with 
    hyperliquid_perps_trading_volume as (
        select
            to_date(to_timestamp_ltz(parse_json(source_json):timestamp::number)) as date,
            parse_json(source_json):daily_volume_total::float as perps_trading_volume
            , 'hyperliquid' as app
            , 'hyperliquid' as chain
            , 'DeFi' as category
        from {{ source("PROD_LANDING", "raw_hyperliquid_perps_trading_volume") }}
    )
select 
    date
    , round(perps_trading_volume, 2) as perps_trading_volume
    , app
    , chain
    , category
from hyperliquid_perps_trading_volume