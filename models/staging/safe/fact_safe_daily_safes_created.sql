{{
    config(
        materialized="table",
        snowflake_warehouse="SAFE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_safe_created") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_safe_created") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:day::string, 10)::date as date
    , f.value:num_safes::number as safes_created
    , 'ethereum' as chain
from latest_data, lateral flatten(input => data) f
