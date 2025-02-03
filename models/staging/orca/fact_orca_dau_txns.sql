{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_orca_dau_txns") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_orca_dau_txns") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    f.value:date::date as date,
    f.value:num_swaps::number as num_swaps,
    f.value:num_traders::number as unique_traders
from latest_data, lateral flatten(input => data) f