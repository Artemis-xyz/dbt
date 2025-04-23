{{
    config(
        materialized = 'table'
    )
}}

select 
    date
    , premine_unlocks_native
    , locked_supply_native
    , circulating_supply_native
    , net_supply_change_native
from {{ source("MANUAL_STATIC_TABLES", "blur_daily_supply_data") }}