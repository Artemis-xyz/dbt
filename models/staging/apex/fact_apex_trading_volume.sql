with
    latest_source_json as (
        select
            max(extraction_date) as extraction_date,
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol
        from
            {{ source("PROD_LANDING", "raw_apex_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        group by 2, 3
    ),
    all_data as (
        select
            to_date(value:"time"::string) as date,
            value:"symbol"::string as symbol,
            value:"open"::float as open,
            value:"close"::float as close,
            value:"volume"::float as volume,
            extraction_date
        from
            {{ source("PROD_LANDING", "raw_apex_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
    ),
    latest_extration as (
        select all_data.date, all_data.symbol, open, close, volume
        from all_data
        join
            latest_source_json
            on all_data.extraction_date = latest_source_json.extraction_date
            and all_data.date = latest_source_json.date
            and all_data.symbol = latest_source_json.symbol
    ),
    volume_by_date as (
        select date, sum(((open + close) / 2) * volume) as trading_volume
        from latest_extration
        group by 1
        order by 1 asc
    )

select *, 'apex' as app, 'apex' as chain, 'DeFi' as category
from volume_by_date
