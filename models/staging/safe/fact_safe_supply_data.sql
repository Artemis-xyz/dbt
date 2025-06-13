{{
    config(
        materialized="table",
        snowflake_warehouse="SAFE",
    )
}}

SELECT
    date,
    core_contributors_supply
    , ecosystem_supply
    , gnosisDAO_treasury_supply
    , joint_treasury_supply
    , safe_foundation_supply
    , safeDAO_treasury_supply
    , strategic_raise_supply
    , user_participation_supply
    , gross_emissions_native
    , premine_unlocks_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native
FROM {{ source('MANUAL_STATIC_TABLES', 'safe_daily_supply_data') }}