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
    left(value:date, 10)::date as date,
    value:overall::number as aggregator_multi_hop_volume,
    value:single::number as aggregator_single_hop_volume
from
    {{ source("PROD_LANDING", "raw_jupiter_aggregator_metrics") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)