with
    max_date as (
        select max(extraction_date) as extraction_date
        from {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }}
    ),
    trading_volume_market as (
        select
            value:total_volume::double as trading_volume,
            value:coin::string as market,
            date(value:time) as date,
            extraction_date
        from
            {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select extraction_date from max_date)
    )
select
    sum(trading_volume) as trading_volume,
    null as market,
    date,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from trading_volume_market
group by date
