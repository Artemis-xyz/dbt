{{
    config(
        materialized="table",
        snowflake_warehouse="METAPLEX"
    )
}}

SELECT 
    date,
    total as premine_unlocks_native,
    total as net_supply_change_native,
    sum(total) over (order by date) as circulating_supply_native
FROM
    {{ source('MANUAL_STATIC_TABLES', 'metaplex_daily_supply_data')}}