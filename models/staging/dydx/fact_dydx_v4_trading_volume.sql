with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_dydx_v4_trading_volume") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_dydx_v4_trading_volume") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            parse_json(data):startedAt::timestamp_ntz as date, -- fmt: off
            parse_json(data):ticker::string as market_pair,
            parse_json(data):usdVolume::float as trading_volume, -- fmt: off
            'dydx_v4' as app,
            'DeFi' as category,
            'dydx_v4' as chain
        from latest_data
    )
select
    null as market_pair,
    date,
    sum(trading_volume) as trading_volume,
    'dydx_v4' as app,
    'DeFi' as category,
    'dydx_v4' as chain
from flattened_data
group by date
