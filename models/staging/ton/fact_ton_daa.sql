with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_ton_daa") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_ton_daa") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select value:"timestamp"::date as date, value:"value"::number as daa
        from latest_data, lateral flatten(input => data)
    )
select date, daa, 'ton' as chain
from flattened_data