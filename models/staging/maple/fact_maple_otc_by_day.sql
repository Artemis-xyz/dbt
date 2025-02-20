{{ 
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}
with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_maple_otc_by_day") }}
    )
select
    value:date::date as date,
    value:timestamp::int as timestamp,
    value:otc_revenue::float as otc_revenue
from
    {{ source("PROD_LANDING", "raw_maple_otc_by_day") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
