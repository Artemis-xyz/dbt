{{
    config(
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_fees",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_pumpswap_fees" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_pumpswap_fees") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            left(f.value:"day",10)::date as date,
            f.value:"daily_lp_fees_usd"::number as daily_lp_fees_usd,
            f.value:"daily_protocol_fees_usd"::number as daily_protocol_fees_usd
        from latest_data, lateral flatten(input => data) as f
    ),
    flattened_data_usd as (
        select
            date,
            daily_lp_fees_usd,
            daily_protocol_fees_usd
        from flattened_data
    )
select 
    date,
    daily_lp_fees_usd,
    daily_protocol_fees_usd
from flattened_data