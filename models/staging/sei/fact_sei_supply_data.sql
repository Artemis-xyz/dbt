{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}

SELECT
    date,
    private_cumulative_supply
    , binance_cumulative_supply
    , team_cumulative_supply
    , foundation_cumulative_supply
    , eco_system_cumulative_supply
    , premine_unlocks_native
    , net_change_native as net_supply_change_native
    , circulating_supply_native
    , burns_native
FROM {{ source('MANUAL_STATIC_TABLES', 'sei_daily_supply_data') }}