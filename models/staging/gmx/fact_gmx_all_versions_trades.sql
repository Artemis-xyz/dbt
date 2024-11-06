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
    price,
    token_address
FROM {{ ref('fact_gmx_v1_price_per_trade') }}

UNION ALL

SELECT
    block_timestamp,
    chain,
    'v2' as version,
    price,
    token_address
FROM {{ ref('fact_gmx_v2_price_per_trade') }}
