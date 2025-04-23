{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
    )
}}

SELECT
    date,
    ecosystem_trading_rewards as gross_emissions,
    airdrop + protocol_development + strategic_partnerships as premine_unlocks,
    gross_emissions + premine_unlocks as net_supply_change,
    sum(net_supply_change) over (order by date) as circulating_supply
FROM {{ source('MANUAL_STATIC_TABLES', 'drift_daily_supply_data')}}