{{
    config( 
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_dau",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_pumpswap_dau" ) }}
    )
    ,latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_pumpswap_dau") }}
        where extraction_date = (select max_date from max_extraction)
    )
    ,flattened_data as (
        select
            left(f.value:"day",10)::date as date,
            f.value:"new_users"::number as new_users,
            f.value:"recurring_users"::number as recurring_users
        from latest_data, lateral flatten(input => data) as f
    )
    ,flattened_data_usd as (
        select
            date,
            new_users,
            recurring_users
        from flattened_data
    )
select 
    date,
    new_users,
    recurring_users,
    new_users + recurring_users as spot_dau
from flattened_data
    