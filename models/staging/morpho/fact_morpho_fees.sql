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
        from {{ source("PROD_LANDING", "raw_morpho_fees") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_morpho_fees") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    f.value:interest::number as interest_usd,
    f.value:blockchain::string as chain,
    'morpho' as app,
    'DeFi' as category
from latest_data, lateral flatten(input => data) f