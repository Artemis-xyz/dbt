{{
    config(
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_txns",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_pumpswap_txns" ) }}   
    )
    ,latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_pumpswap_txns") }}
        where extraction_date = (select max_date from max_extraction)
    )
    ,flattened_data as (
        select
            left(f.value:"day",10)::date as date,
            f.value:"daily_swaps"::number as daily_txns
        from latest_data, lateral flatten(input => data) as f
    )
    ,flattened_data_usd as (
        select
            date,
            daily_txns
        from flattened_data
    )
select 
    date,
    daily_txns
from flattened_data
        