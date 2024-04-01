with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_osmosis_trading_fees") }}
    ),
    raw as (
        select date(value:time::date) as date, value:"fees_spent"::float as trading_fees
        from
            {{ source("PROD_LANDING", "raw_osmosis_trading_fees") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    )
select raw.date, 'osmosis' as chain, trading_fees
from raw
where date < to_date(sysdate())
