{{
    config(
        materialized='table',
        snowflake_warehouse='PUMPFUN',
        database='PUMPFUN',
        schema='raw',
        alias='fact_pumpfun_dailyrevenues',
    )
 }}

WITH fee_recipients AS (
    SELECT fee_recipient
    FROM {{ ref('fact_pumpfun_feeaccounts') }}
),
change AS (
    SELECT 
        date_trunc(day, block_timestamp) AS date,
        SUM(balance - pre_balance) AS daily_change
    FROM solana_flipside.core.fact_sol_balances
    WHERE owner IN (SELECT fee_recipient FROM fee_recipients)
    AND (balance - pre_balance) > 0
    AND (balance - pre_balance) < 1000 --filter out edge cases
    AND SUCCEEDED
    GROUP BY 1
),
price AS (
    SELECT 
        date_trunc(day, hour) AS date,
        AVG(price) AS avg_price
    FROM solana_flipside.price.ez_prices_hourly
    WHERE token_address = 'So11111111111111111111111111111111111111112'
    GROUP BY 1
)
SELECT 
    date,
    daily_change AS Revenue_SOL,
    daily_change * avg_price AS Revenue_USD
FROM change
JOIN price Using(date)
WHERE date < CURRENT_DATE()