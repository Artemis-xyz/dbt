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
            max(parse_json(source_json):daily_volume_total::double) as trading_volume
            , null as market
            , date(to_timestamp(parse_json(source_json):timestamp::number)) as date
            , max(extraction_date) as extraction_date
        from LANDING_DATABASE.PROD_LANDING.raw_hyperliquid_perps_trading_volume
        where date(extraction_date) > '2025-05-24'
        group by date(to_timestamp(parse_json(source_json):timestamp::number))
    )
select
    trading_volume
    , date
    , 'hyperliquid' as app
    , 'hyperliquid' as chain
    , 'DeFi' as category
from trading_volume_market