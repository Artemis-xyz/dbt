with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_bluefin_trading_volume") }}
    ),
    raw_data as (
        select
            value:"date"::date as date,
            value:"volume"::float as trading_volume,
            value:"market"::string as market
        from
            {{ source("PROD_LANDING", "raw_bluefin_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select
    date,
    'bluefin' as app,
    'sui' as chain,
    'DeFi' as category,
    sum(trading_volume) as trading_volume
from raw_data
group by 1
