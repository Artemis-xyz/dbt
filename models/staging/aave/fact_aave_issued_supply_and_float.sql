{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
    )
}}

WITH min_max_dates AS (
    SELECT 
        MIN(BLOCK_TIMESTAMP)::DATE AS min_date,
        MAX(BLOCK_TIMESTAMP)::DATE AS max_date
    FROM {{ source("ETHEREUM_FLIPSIDE", "fact_token_balances" ) }} 
    WHERE contract_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
      AND user_address IN (
          lower('0x317625234562b1526ea2fac4030ea499c5291de4'),  -- migration
          lower('0x25F2226B597E8F9514B3F68F00f494cF4f286491')   -- foundation
      )
),
generated_dates AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS day_offset
    FROM TABLE(GENERATOR(ROWCOUNT => 2000)) 
),
date_spine AS (
    SELECT 
        DATEADD(DAY, day_offset, (SELECT min_date FROM min_max_dates)) AS date
    FROM generated_dates
    WHERE DATEADD(DAY, day_offset, (SELECT min_date FROM min_max_dates)) <= (SELECT max_date FROM min_max_dates)
),
migration_contract AS (
    SELECT 
        BLOCK_TIMESTAMP::date AS date,
        MEDIAN(BALANCE_TOKEN) / 1e18 AS balance
    FROM {{ ref("fact_ethereum_address_balances_by_token") }}
    WHERE address = lower('0x317625234562b1526ea2fac4030ea499c5291de4')
      AND contract_address = lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
    GROUP BY date
),
foundation_balance AS (
    SELECT 
        BLOCK_TIMESTAMP::date AS date,
        MEDIAN(BALANCE_TOKEN) / 1e18 AS balance
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
    16000000 as max_supply,
    16000000 as total_supply_to_date,
    0 as uncreated_tokens,
    0 as cumulative_burns,
    MEDIAN(migration_balance) as locked_balance,
    MEDIAN(foundation_balance) as foundation_balance,
    16000000 - MEDIAN(foundation_balance) AS issued_supply,
    16000000 - MEDIAN(foundation_balance) - MEDIAN(migration_balance) AS circulating_supply
FROM filled_data
GROUP BY date
ORDER BY date