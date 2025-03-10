{{
    config(
        materialized='table',
        snowflake_warehouse='CURVE',
    )
}}

-- Used for Convex for calculating LP Token Prices since we need price of all Curve related tokens (including LP tokens)

SELECT
    c.date,
    contract_address,
    t.tvl / NULLIF(c.circulating_supply,0) as price
FROM
    {{ ref('fact_curve_pools_circulating_supply') }} c
    LEFT JOIN {{ ref('fact_curve_tokens_tvl') }} t ON c.date = t.date AND c.contract_address = t.address
WHERE price is not null

UNION ALL

SELECT date, token as contract_address, price FROM
    (
        SELECT
            c.date,
            c.contract_address as token,
            t.address as pool_address,
            t.tvl / NULLIF(c.circulating_supply,0) as price
        FROM
            {{ ref('fact_curve_pools_circulating_supply') }} c
            JOIN {{ ref('dim_curve_pools') }} cp ON lower(cp.token) = lower(c.contract_address)
            JOIN {{ ref('fact_curve_tokens_tvl') }} t ON c.date = t.date AND lower(cp.pool_address) = lower(t.address)
        WHERE price is not null
    )