with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_hyperliquid_trading_volume") }}
        order by extraction_date desc
        limit 1
    ),

    trading_volume_market as (
        select
            value:total_volume::double as trading_volume,
            value:coin::string as market,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category,
            date(value:time) as date
        from latest_source_json, lateral flatten(input => parse_json(source_json))
    )

select trading_volume, market, date, app, chain, category
from trading_volume_market
union
select
    sum(trading_volume) as trading_volume,
    null as market,
    date,
    'hyperliquid' as app,
    'hyperliquid' as chain,
    'DeFi' as category
from trading_volume_market
group by date
