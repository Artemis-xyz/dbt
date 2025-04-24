{{
    config(
        materialized="table",
        snowflake_warehouse="BLAST",
    )
}}

select 
    date
    , premine_unlocks_native
    , circulating_supply_native
from {{ source("MANUAL_STATIC_TABLES", "blast_daily_supply_data") }}