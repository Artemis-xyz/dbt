{{
    config(
        materialized="table",
        snowflake_warehouse="ORCA",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_scroll_dex_volumes_daily") }}
        
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_scroll_dex_volumes_daily") }}
        where extraction_date = (select max_date from max_extraction)
    )
select
    left(f.value:block_date, 10)::date as date,
    f.value:daily_volume::number as daily_volume,
from latest_data, lateral flatten(input => data) f