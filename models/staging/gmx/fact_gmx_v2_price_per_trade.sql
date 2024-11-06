{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

SELECT
    block_timestamp,
    tx_hash,
    event_index,
    chain,
    price,
    token_address,
    symbol
FROM {{ ref('fact_gmx_v2_arbitrum_trade_prices') }}
UNION ALL
SELECT
    block_timestamp,
    tx_hash,
    event_index,
    chain,
    price,
    token_address,
    symbol
FROM {{ ref('fact_gmx_v2_avalanche_trade_prices') }}   
