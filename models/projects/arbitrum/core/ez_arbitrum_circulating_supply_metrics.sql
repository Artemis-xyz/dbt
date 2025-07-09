{{
    config(
        materialized="table",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="core",
        alias="ez_circulating_supply_metrics",
    )
}}

SELECT
    date,
    airdrop_amount + investor_team_unlock_amount + arbitrum_foundation_unlocks_amount + dao_emissions_amount as daily_emissions,
    0 as daily_burns,
    daily_emissions - daily_burns as daily_net_emissions,
    sum(daily_net_emissions) over (order by date rows between unbounded preceding and current row) as circulating_supply,
    total_vested_supply
FROM {{ ref("fact_arbitrum_all_supply_events") }}