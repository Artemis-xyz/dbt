{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics",
    )
}}

-- Simplified ez metrics table that aggregates data for eigenlayer
WITH 
eigenlayer_aggregated AS (
SELECT 
    date,
    protocol,
    category,
    SUM(num_restaked_eth) AS num_restaked_eth,
    SUM(amount_restaked_usd) AS amount_restaked_usd
FROM {{ref('fact_eigenlayer_restaked_assets')}}
GROUP BY date, protocol, category
),
eigenlayer_supply_data AS (
SELECT
    date,
    emissions_native,
    premine_unlocks_native,
    net_supply_change_native,
    circulating_supply
FROM {{ ref('fact_eigenlayer_supply_data') }}
)

SELECT 
    a.date,
    protocol,
    category,
    num_restaked_eth,
    amount_restaked_usd,
    -- Calculate net daily change using LAG()
    num_restaked_eth - LAG(num_restaked_eth) 
        OVER (ORDER BY a.date) AS num_restaked_eth_net_change,
    amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (ORDER BY a.date) AS amount_restaked_usd_net_change,
    emissions_native,
    premine_unlocks_native,
    net_supply_change_native,
    circulating_supply
FROM eigenlayer_aggregated a
LEFT JOIN eigenlayer_supply_data s ON a.date = s.date
ORDER BY a.date