{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

SELECT
    DATE(block_timestamp) AS date,
    p.asset,
    net_interest AS net_interest
FROM {{ ref('fact_maple_interest_v2') }}
UNION ALL
SELECT
    DATE(block_timestamp) AS date,
    p.asset,
    fee_earned AS net_interest
FROM {{ ref('fact_maple_interest_v1') }}