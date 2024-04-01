with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_tron_fees_revenue") }}
    )
select
    value:"date"::date as date,
    'tron' as chain,
    value:"fees"::float as fees,
    value:"fees"::float as revenue
from
    {{ source("PROD_LANDING", "raw_tron_fees_revenue") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
