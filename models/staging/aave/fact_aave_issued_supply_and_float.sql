{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
    )
}}

WITH date_spine AS (
    SELECT date
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2020-12-03' AND TO_DATE(SYSDATE())
),
migration_contract AS (
    SELECT 
        BLOCK_TIMESTAMP::date AS date,
        MAX_BY(balance_token, block_timestamp) / 1e18 AS balance
    FROM {{ ref("fact_ethereum_address_balances_by_token") }}
    WHERE address = lower('0x317625234562b1526ea2fac4030ea499c5291de4')
      AND contract_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
    GROUP BY date
),
foundation_balance AS (
    SELECT 
        BLOCK_TIMESTAMP::date AS date,
        MAX_BY(balance_token, block_timestamp) / 1e18 AS balance
    FROM {{ ref("fact_ethereum_address_balances_by_token") }}
    WHERE address = lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491')
      AND contract_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
    GROUP BY date
),
joined_data AS (
    SELECT 
        d.date,
        mc.balance AS migration_balance,
        fb.balance AS foundation_balance
    FROM date_spine d
    LEFT JOIN migration_contract mc ON d.date = mc.date
    LEFT JOIN foundation_balance fb ON d.date = fb.date
),
filled_data AS (
    SELECT 
        date,
        LAST_VALUE(migration_balance IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS migration_balance,
        LAST_VALUE(foundation_balance IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS foundation_balance
    FROM joined_data
)
SELECT 
    date,
    16000000 AS max_supply,
    16000000 AS total_supply_to_date,
    migration_balance AS uncreated_tokens,
    0 AS cumulative_burns,
    foundation_balance AS foundation_balance,
    16000000 - foundation_balance - migration_balance AS issued_supply,
    16000000 - foundation_balance - migration_balance AS circulating_supply
FROM filled_data
ORDER BY date