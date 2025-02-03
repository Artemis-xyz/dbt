{{
    config(
        materialized="table",
        snowflake_warehouse="LAYERZERO"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_layerzero_cross_chain_volume") }}
    )
select
    value:txn_date::date as date,
    value:cross_chain_usd_volume::number as bridge_volume
from
    {{ source("PROD_LANDING", "raw_layerzero_cross_chain_volume") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)