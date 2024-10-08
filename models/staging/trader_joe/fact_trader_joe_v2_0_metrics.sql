{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'trader_joe'
    )
}}

SELECT
    date,
    chain,
    version,
    total_volume,
    total_fees,
    protocol_fees
FROM {{ref('fact_trader_joe_arbitrum_v2_0_metrics')}}

UNION ALL

SELECT
    date,
    chain,
    version,
    total_volume,
    total_fees,
    protocol_fees
FROM {{ref('fact_trader_joe_avalanche_v2_0_metrics')}}