{{
    config(
        materialized="table",
        snowflake_warehouse="JUPITER"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_jupiter_aggregator_unique_traders") }}
    )
select
    value:date::date as date,
    value:num_traders::number as unique_aggregator_traders
from
    {{ source("PROD_LANDING", "raw_jupiter_aggregator_unique_traders") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)