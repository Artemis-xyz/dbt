{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

-- Simplified ez metrics table that aggregates data by chain from the fact table
WITH chain_aggregates AS (
    SELECT 
        date,
        chain,
        protocol,
        category,
        SUM(num_restaked_eth) AS num_restaked_eth,
        SUM(amount_restaked_usd) AS amount_restaked_usd
    FROM {{ref('fact_eigenlayer_restaked_assets')}}
    GROUP BY date, chain, protocol, category
)

SELECT 
    date,
    protocol,
    category,
    chain,
    num_restaked_eth as tvl_native,
    amount_restaked_usd as tvl,
    -- Calculate net daily change using LAG()
    num_restaked_eth - LAG(num_restaked_eth) 
        OVER (ORDER BY date) AS tvl_native_net_change,
    amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (ORDER BY date) AS tvl_net_change
FROM chain_aggregates
ORDER BY date