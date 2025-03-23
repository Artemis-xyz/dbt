{{
    config(
        materialized="table",
        snowflake_warehouse="SONIC",
    )
}}

SELECT
    date,
    circulating_supply_at_launch,
    airdrop_supply,
    ongoing_funding_supply,
    ftm_block_rewards_supply,
    sonic_block_rewards_supply,
    emissions_native,
    premine_unlocks_native,
    net_supply_change_native,
    sum(emissions_native) over (order by date asc rows between unbounded preceding and current row)
        as circulating_supply_native
    -- emissions data will (starts 6 months after TGE - 2025-07-06) and no burns data yet for sonic
FROM {{ source('MANUAL_STATIC_TABLES', 'sonic_daily_supply_data') }}
