with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_akash_burns_native") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_akash_burns_native") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select date(to_timestamp(value:date::number / 1000)) as date,
        value:"total"::float as total_burned_native
        from latest_data, lateral flatten(input => data)
    )
select date, total_burned_native, 'akash' as chain
from flattened_data