{{
    config(
        materialized="table",
        snowflake_warehouse="VIRTUALS",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_virtuals_fees") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_virtuals_fees") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:dt::string, 10)::date as date,
    f.value:fee_fun::number as fee_fun_native,
    f.value:fee_fun_usd::number as fee_fun_usd,
    f.value:tax_usd::number as tax_usd,
    f.value:revenue::number as fees,
    'virtuals' as app,
    'base' as chain,
    'DeFi' as category
from latest_data, lateral flatten(input => data) f
