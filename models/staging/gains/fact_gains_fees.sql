{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_gains_fees") }}
    )
select
    left(value:day, 10)::date as date,
    value:all_fees::number as fees,
    value:revenue::number as revenue
from
    {{ source("PROD_LANDING", "raw_gains_fees") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)