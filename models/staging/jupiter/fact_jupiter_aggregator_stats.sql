{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_jupiter_aggregator_metrics") }}
    )
select
    value:date::date as date,
    value:overall::number as overall,
    value:single::number as single
from
    {{ source("PROD_LANDING", "raw_jupiter_aggregator_metrics") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)