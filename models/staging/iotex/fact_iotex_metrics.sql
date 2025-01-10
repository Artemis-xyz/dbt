{{
    config(
        materialized="table",
        snowflake_warehouse="IOTEX"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_iotex_metrics") }}
    )
select
    value:date::date as date,
    value:fees::number as fees_native,
    value:daily_active_addresses::number as dau,
    value:daily_txns::number as txns
from
    {{ source("PROD_LANDING", "raw_iotex_metrics") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)