with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_akash_burns") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_akash_burns") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(value:day::number / 1000)) as date,
            value:"total"::float as revenue_native
        from latest_data, lateral flatten(input => data)
    )
select date, coalesce(revenue_native, 0) as revenue_native
from flattened_data
order by date desc
