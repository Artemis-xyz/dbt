{{
    config(
        materialized="table",
    )
}}

select 
    date, 
    gross_emissions_native, 
    circulating_supply_native
from {{ source("MANUAL_STATIC_TABLES", "chiliz_daily_supply_data") }}