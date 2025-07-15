{{ config(materialized="table", snowflake_warehouse="HYPERLIQUID") }}

with
    max_date as (
        select max(extraction_date) as extraction_date
        from {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }}
    ),
    trading_volume_market as (
        -- historical data from stats.hyperliquid.xyz
        select
            try_to_date(f.value:time::string) as date
            , sum(try_to_numeric(f.value:total_volume::string)) as trading_volume
        from LANDING_DATABASE.PROD_LANDING.raw_hyperliquid_trading_volume t,
            lateral flatten(input => parse_json(t.source_json)) as f
        where date(extraction_date) = '2025-06-19'
        group by date

        union all

        -- point in time data from hyperliquid API
        select
            date
            , perps_trading_volume as trading_volume
        from {{ ref("fact_hyperliquid_perps_trading_volume") }}
        where date > '2025-06-17'
    )
select
    date
    , trading_volume
    , 'hyperliquid' as app
    , 'hyperliquid' as chain
    , 'DeFi' as category
from trading_volume_market