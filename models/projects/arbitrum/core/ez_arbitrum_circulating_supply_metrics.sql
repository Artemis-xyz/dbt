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
    airdrop_amount + team_foundation_dao_unlock_amount + investor_unlock_amount + arbitrum_foundation_unlocks_amount as daily_emissions,
    0 as daily_burns,
    daily_emissions - daily_burns as daily_net_emissions,
    sum(daily_net_emissions) over (order by date rows between unbounded preceding and current row) as circulating_supply
FROM {{ ref("fact_arbitrum_all_supply_events") }}