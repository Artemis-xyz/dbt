{{
    config(
        materialized="table",
        snowflake_warehouse="SAFE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_safe_tvl_by_chain") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_safe_tvl_by_chain") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:day::string, 10)::date as date
    , f.value:blockchain::string as chain
    , f.value:total_aum::number as tvl
from latest_data, lateral flatten(input => data) f
