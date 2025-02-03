{{
    config(
        materialized="table",
        snowflake_warehouse="HELIUM"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_helium_new_mobile_subscribers") }}
    )
select
    left(value:date, 10)::date as date,
    value:subscribers::number as new_subscribers
from
    {{ source("PROD_LANDING", "raw_helium_new_mobile_subscribers") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)