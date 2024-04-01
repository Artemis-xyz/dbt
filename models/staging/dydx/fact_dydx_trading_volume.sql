with
    data as (
        select
            source_url,
            parse_json(max_by(source_json, extraction_date)) as source_json,
            regexp_substr(source_url, 'candles/([^/?]+)', 1, 1, 'e', 1) as market_pair,
            date(
                regexp_substr(source_url, 'fromISO=([^&]+)', 1, 1, 'e', 1)
            ) as from_date
        from {{ source("PROD_LANDING", "raw_dydx_trading_volume") }}
        group by source_url
    ),

    trading_volume_market_pair as (
        select
            market_pair,
            dateadd(day, 1, from_date) as date,
            source_json:"usdVolume"::double as trading_volume,
            'dydx' as app,
            'DeFi' as category,
            null as chain
        from data
    )

select
    null as market_pair,
    date,
    sum(trading_volume) as trading_volume,
    'dydx' as app,
    'DeFi' as category,
    null as chain
from trading_volume_market_pair
group by date
union
select *
from trading_volume_market_pair
