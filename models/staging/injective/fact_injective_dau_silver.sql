{{config(snowflake_warehouse="INJECTIVE")}}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_injective_dau") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_injective_dau") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            date(to_timestamp(f.value:date::number / 1000)) as date,
            f.value:dau::number as dau
        from latest_data, lateral flatten(input => data) as f
    )
select date, dau, 'injective' as chain
from flattened_data
where date < to_date(sysdate())
order by date desc
