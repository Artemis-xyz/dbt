with
    max_date as (
        select max(extraction_date) as extraction_date
        from {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }}
    ),
    trading_volume_market as (
        select
            sum(value:total_volume::double) as trading_volume
            , max(value:coin::string) as market
            , date(value:time) as date
            , max(extraction_date) as extraction_date
        from
            {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        where date(extraction_date) = '2025-05-28'
        group by date

        union all

        select
            perps_trading_volume as trading_volume
            , null as market
            , date
            , null as extraction_date
        from {{ ref("fact_hyperliquid_perps_trading_volume") }}
        where date > '2025-05-24'
    )
select
    trading_volume
    , date
    , 'hyperliquid' as app
    , 'hyperliquid' as chain
    , 'DeFi' as category
from trading_volume_market