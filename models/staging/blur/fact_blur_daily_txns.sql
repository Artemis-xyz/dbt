{{
    config(
        materialized = 'table'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_blur_daily_txns") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_blur_daily_txns") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(SUBSTR(f.value:time, 0, 10)::date)) as date,
            f.value:trades::number as daily_txns
        from latest_data, lateral flatten(input => data) as f
    )
select date, daily_txns, 'ethereum' as chain, 'blur' as app
from flattened_data
where date < to_date(sysdate())
order by date desc