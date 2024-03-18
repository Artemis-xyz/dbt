with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_injective_revenue") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_injective_revenue") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(f.value:date::number / 1000)) as date,
            f.value:revenue::float as revenue
        from latest_data, lateral flatten(input => data) as f
    )
select date, revenue, 'injective' as chain
from flattened_data
where date < to_date(sysdate())
order by date desc
