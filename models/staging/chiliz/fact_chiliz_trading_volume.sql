with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_chiliz_trading_volume") }}
    ),

    data as (
        select
            source_url,
            parse_json(source_json) as source_json,
            regexp_substr(source_url, 'symbol=([^&?]+)', 1, 1, 'e', 1) as market_pair
        from {{ source("PROD_LANDING", "raw_chiliz_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    ),

    trading_volume_market_pair as (
        select
            null as app,
            'chiliz' as chain,
            null as category,
            market_pair,
            value[5]::double as trading_volume,
            date(value[0]) as date
        from data, lateral flatten(input => source_json)
    )

select app, chain, category, market_pair, date, trading_volume
from trading_volume_market_pair
union
select
    null as app,
    'chiliz' as chain,
    null as category,
    null as market_pair,
    date,
    sum(trading_volume) as trading_volume
from trading_volume_market_pair
group by date
