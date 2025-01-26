{{
    config(
        materialized="table",
        snowflake_warehouse="HELIUM"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_helium_mints") }}
    )
select
    left(value:block_date, 10)::date as date,
    value:treasury::number as treasury_mints,
    value:holders::number as holders_mints,
    value:total::number as mints_native
from
    {{ source("PROD_LANDING", "raw_helium_mints") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)