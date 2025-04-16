{{
    config(
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_volume",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_pumpswap_volume" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_pumpswap_volume") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            left(f.value:"date",10)::date as date,
            f.value:"daily_volume_usd"::number as daily_volume_usd
        from latest_data, lateral flatten(input => data) as f
    ),
    flattened_data_usd as (
        select
            date,
            daily_volume_usd
        from flattened_data
    )
select 
    date,
    daily_volume_usd
from flattened_data