{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_tvl'
    )
}}

with combined_tvl as (
    SELECT * FROM {{ ref('fact_aerodrome_v1_tvl') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_aerodrome_v2_tvl') }}
)
SELECT
    date,
    chain,
    version,
    pool_address,
    token_address,
    token_balance,
    token_balance_usd
FROM combined_tvl
