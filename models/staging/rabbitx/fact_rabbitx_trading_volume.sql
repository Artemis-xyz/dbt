with
    latest_source_jsons as (
        select
            extraction_date,
            source_url,
            source_json,
            rank() over (
                partition by (source_url, date(extraction_date))
                order by extraction_date desc
            ) as rnk
        from {{ source("PROD_LANDING", "raw_rabbitx_trading_volume") }}
    ),

    trading_volume_market_pair as (
        select
            value:average_daily_volume::double as trading_volume,
            value:id::string as market_pair,
            'rabbitx' as app,
            null as chain,
            'DeFi' as category,
            dateadd(day, -1, date(extraction_date)) as date
        from latest_source_jsons, lateral flatten(input => parse_json(source_json))
        where rnk = 1
    )

select trading_volume, market_pair, date, app, chain, category
from trading_volume_market_pair
union
select
    sum(trading_volume) as trading_volume,
    null as market_pair,
    date,
    'rabbitx' as app,
    null as chain,
    'DeFi' as category
from trading_volume_market_pair
group by date
