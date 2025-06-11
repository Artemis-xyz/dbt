{{
    config({
        "materialized": "table",
        "alias": "fact_dydx_supply_data",
        "snowflake_warehouse": "DYDX",
    })
}}

SELECT
    date,
    premine_unlocks::float as premine_unlocks_native,
    circulating_supply_native::float as circulating_supply_native
FROM {{ source('MANUAL_STATIC_TABLES', 'dydx_daily_supply_data') }}
WHERE date <= CURRENT_DATE() -- Only include data up to the current date
ORDER BY date