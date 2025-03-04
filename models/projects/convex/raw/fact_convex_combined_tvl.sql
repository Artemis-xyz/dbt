{{ config(
    materialized='table',
    database='CONVEX',
    schema='raw',
    alias='fact_convex_combined_tvl'
) }}

with agg as (
    SELECT
        date,
        contract_address,
        symbol,
        balance_native,
        balance_usd
    FROM
        {{ ref('fact_convex_cvx_tokens_tvl') }}
    UNION ALL
    SELECT
        date,
        contract_address,
        symbol,
        balance_native,
        balance_usd
    FROM
        {{ ref('fact_convex_gauge_tokens_tvl') }}
)
SELECT
    date,
    contract_address,
    symbol,
    SUM(balance_native) as tvl_native,
    SUM(balance_usd) as tvl
FROM
    agg
GROUP BY
    1,
    2,
    3