with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_avantis_trading_volume") }}
    ),
    raw_data as (
        select value:"date"::date as date, value:"volume"::float as trading_volume,
        from
            {{ source("PROD_LANDING", "raw_avantis_trading_volume") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select date, 'avantis' as app, 'base' as chain, 'DeFi' as category, trading_volume
from raw_data
