with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_avantis_unique_traders") }}
    ),
    raw_data as (
        select value:"date"::date as date, value:"traderCount"::float as unique_traders,
        from
            {{ source("PROD_LANDING", "raw_avantis_unique_traders") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select date, 'avantis' as app, 'base' as chain, 'DeFi' as category, unique_traders
from raw_data
