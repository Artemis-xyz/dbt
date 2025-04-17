{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with 
    hyperliquid_spot_trading_volume as (
        select
            day as date
            , hyperliquid_volume as spot_trading_volume
        from {{ source('MANUAL_STATIC_TABLES', 'hyperliquid_spot_trading_volume') }}
    
    union all

        select
            extraction_date::date as date,
            parse_json(source_json):daily_volume_total::float as spot_trading_volume
        from {{ source("PROD_LANDING", "raw_hyperliquid_spot_trading_volume") }}
    )
select 
    date
    , round(spot_trading_volume, 2) as spot_trading_volume
from hyperliquid_spot_trading_volume