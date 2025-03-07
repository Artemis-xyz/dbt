{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="core",
        alias="ez_circulating_supply_metrics",
    )
}}

SELECT
    date,
    airdrop_supply + retropgf_supply + gov_grants_supply + core_contributor_unlocks_supply + investor_unlocks_supply + base_grant_supply + private_sale_supply as daily_emissions,
    0 as daily_burns,
    daily_emissions - daily_burns as daily_net_emissions,
    sum(daily_net_emissions) over (order by date rows between unbounded preceding and current row) as circulating_supply
FROM {{ ref("fact_optimism_all_supply_events") }}