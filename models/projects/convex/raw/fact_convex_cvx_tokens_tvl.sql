{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_cvx_tokens_tvl'
    )
}}

with agg as (
    SELECT
        date,
        contract_address,
        balance
    FROM
        {{ ref('fact_convex_cvxcrv_balance') }}
    UNION ALL
    SELECT
        date,
        contract_address,
        balance
    FROM
        {{ ref('fact_convex_cvxfxs_balance') }}
)
SELECT
    date,
    contract_address,
    balance,
    CASE
        WHEN lower(contract_address) = lower('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2') THEN 'cvxCRV'
        ELSE 'cvxFXS'
    END AS symbol,
    CASE
        WHEN lower(contract_address) = lower('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2') THEN crv.price
        ELSE fxs.price
    END AS price_adj,
    balance as balance_native,
    balance * price_adj as balance_usd
FROM
    agg
    LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} crv ON crv.hour = agg.date
    AND crv.token_address = lower('0xD533a949740bb3306d119CC777fa900bA034cd52')
    LEFT JOIN {{ source('ETHEREUM_FLIPSIDE_PRICE', 'ez_prices_hourly') }} fxs ON fxs.hour = agg.date
    AND fxs.token_address = lower('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0')