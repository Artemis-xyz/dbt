{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GMX'
    )
}}

SELECT
    block_timestamp,
    chain,
    'v1' as version,
    token_address,
    price
FROM {{ ref('fact_gmx_v1_price_per_trade') }}

UNION ALL

SELECT
    block_timestamp,
    chain,
    'v2' as version,
    token_address,
    price
FROM {{ ref('fact_gmx_v2_price_per_trade') }}
