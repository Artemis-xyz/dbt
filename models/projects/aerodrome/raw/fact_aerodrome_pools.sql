{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_pools'
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_aerodrome_pools") }}
    )
select
    value:date::date as date,
    value:daily_count::number as daily_count,
    value:cumulative_count::number as cumulative_count
from
    {{ source("PROD_LANDING", "raw_aerodrome_pools") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)
