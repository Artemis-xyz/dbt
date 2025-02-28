{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_gauge_tokens_tvl'
    )
}}

with lp_token_prices as (
    SELECT date, contract_address, price FROM {{ ref('fact_curve_lp_token_prices') }}
)
SELECT
    t.date,
    t.token_address as contract_address,
    COALESCE(t.name, ep0.symbol) as symbol,
    SUM(t.balance_native) as balance_native,
    SUM(t.balance_native * coalesce(lp.price, ep0.price)) as balance_usd
FROM
    convex.prod_raw.fact_convex_staked_tvl_by_token t
    LEFT JOIN {{ ref('dim_curve_pools') }} cp on lower(cp.token) = lower(t.token_address)
    LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} ep0 on (
        ep0.hour = t.date
        AND lower(ep0.token_address) = lower(cp.coin_0)
    )
    LEFT JOIN lp_token_prices lp on (
        t.date = lp.date
        AND lower(lp.contract_address) = lower(t.token_address)
    )
WHERE
    1 = 1
    AND NOT (
        ep0.price is null
        AND lp.price is null
    )
    AND t.date < to_date(sysdate())
GROUP BY
    1,
    2,
    3