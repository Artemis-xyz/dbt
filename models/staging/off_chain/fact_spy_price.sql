with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_spy_price") }}
    ),
    data as (
        select parse_json(source_json) data
        from {{ source("PROD_LANDING", "raw_spy_price") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    date(key) as date,
    value::double as price,
    'spy' as chain,
    null as category,
    null as app
from data, lateral flatten(input => data:"Adj Close")
