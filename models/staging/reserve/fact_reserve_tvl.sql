{{
    config(
        materialized="table",
        snowflake_warehouse="RESERVE",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_reserve_tvl") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_reserve_tvl") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:last_update_time::date as date,
    f.value:total_tvl::number as tvl
from latest_data, lateral flatten(input => data) f
