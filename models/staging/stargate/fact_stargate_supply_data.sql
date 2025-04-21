{{
    config(
        materialized="table",
        snowflake_warehouse="SEI",
    )
}}

SELECT
    date,
    team_supply
    , investors_supply
    , protocol_launch_supply
    , bonding_curve_supply
    , initial_emissions_program_supply
    , bonding_curve_dexs
    , community_supply
    , gross_emissions_native
    , premine_unlocks_native
    , net_change_supply_native as net_supply_change_native
    , circulating_supply_native
    , burns_native
FROM {{ source('MANUAL_STATIC_TABLES', 'stargate_daily_supply_data') }}