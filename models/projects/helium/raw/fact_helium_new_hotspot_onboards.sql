{{
    config(
        materialized="table",
        snowflake_warehouse="HELIUM"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_helium_new_hotspot_onboards") }}
    )
select
    left(value:date, 10)::date as date,
    value:total_onboarded::number as device_onboards
from
    {{ source("PROD_LANDING", "raw_helium_new_hotspot_onboards") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)