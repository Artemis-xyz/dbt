{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

SELECT
    DATE(block_timestamp) AS date,
    asset,
    pool_name,
    net_interest AS net_interest_usd,
    net_interest_native AS net_interest_native,
    platform_fees_usd,
    platform_fees_native,
    delegate_fees_usd,
    delegate_fees_native
FROM {{ ref('fact_maple_interest_v2') }}
UNION ALL
SELECT
    DATE(block_timestamp) AS date,
    asset,
    pool_name,
    interest_to_lps_usd AS net_interest_usd,
    interest_to_lps_native AS net_interest_native,
    NULL as platform_fees_usd,
    NULL as platform_fees_native,
    NULL as delegate_fees_usd,
    NULL as delegate_fees_native
FROM {{ ref('fact_maple_interest_v1') }}