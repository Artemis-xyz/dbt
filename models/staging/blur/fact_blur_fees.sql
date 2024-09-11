{{
    config(
        materialized = 'table'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_blur_fees") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_blur_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(SUBSTR(f.value:time, 0, 10)::date)) as date,
            f.value:fees_paid::number as fees
        from latest_data, lateral flatten(input => data) as f
    )
select date, fees, 'ethereum' as chain, 'blur' as app
from flattened_data
where date < to_date(sysdate())
order by date desc