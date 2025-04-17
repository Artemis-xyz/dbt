{{
    config(
        materialized="table",
        snowflake_warehouse="MORPHO",
        unique_key="date"
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_morpho_data") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_morpho_data") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    f.value:daily_active_addresses::number as dau,
    f.value:daily_transactions::number as txns,
    f.value:borrow_amount_usd::number as borrow_amount_usd,
    f.value:collat_amount_usd::number as collat_amount_usd,
    f.value:supply_amount_usd::number as supply_amount_usd,
    f.value:interest::number as fees_usd,
    f.value:blockchain::string as chain,
    'morpho' as app,
    'DeFi' as category
from latest_data, lateral flatten(input => data) f