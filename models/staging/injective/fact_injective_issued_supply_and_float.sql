{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
    )
}}


WITH latest_rewards_json AS (
  SELECT 
    PARSE_JSON(SOURCE_JSON) AS parsed_json
  FROM LANDING_DATABASE.PROD_LANDING.RAW_INJECTIVE_MINTS
  QUALIFY ROW_NUMBER() OVER (ORDER BY EXTRACTION_DATE DESC) = 1
),
rewards_data AS (
  SELECT 
    TO_DATE(TO_TIMESTAMP_LTZ(f.value:"date"::NUMBER / 1000)) AS date,
    f.value:"staker_rewards"::FLOAT AS staker_rewards
  FROM latest_rewards_json,
       LATERAL FLATTEN(input => parsed_json) f
),
cumulative_rewards AS (
  SELECT 
    date,
    staker_rewards,
    SUM(staker_rewards) OVER (ORDER BY date) AS cumulative_rewards
  FROM rewards_data
),
foundation_balance_raw AS (
    SELECT 
        block_timestamp::date AS date,
        MAX(balance / 1e18) AS foundation_balance
    FROM ethereum_flipside.core.fact_token_balances
    WHERE 
        lower(user_address) = lower('0x7E233EAfC76243474369bd080238fD6EB36A73CE')
        AND lower(contract_address) = lower('0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30')
    GROUP BY block_timestamp::date
),
foundation_balance_filled AS (
    SELECT 
        m.date,
        LAST_VALUE(f.foundation_balance IGNORE NULLS) OVER (
            ORDER BY m.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS foundation_wallet_balance
    FROM injective.prod_core.ez_metrics m
    LEFT JOIN foundation_balance_raw f
      ON m.date = f.date
),
vesting_data AS (
SELECT 
    date,
    SUM(outflows) AS outflows,
    SUM(SUM(outflows)) OVER (ORDER BY date) AS remaining_vesting_balance
FROM PC_DBT_DB.PROD.FACT_INJECTIVE_UNLOCKS
GROUP BY date
)
SELECT 
    m.date,
    m.revenue,
    m.circulating_supply_native as max_supply_to_date,
    0.0 as uncreated_tokens,
    m.circulating_supply_native as total_supply,
    m.BURNED_FEE_ALLOCATION as burned_inj,
    f.foundation_wallet_balance as foundation_wallet_balance,
    100000000 + COALESCE(cr.cumulative_rewards, 0) - f.foundation_wallet_balance AS issued_supply,
    COALESCE(v.remaining_vesting_balance, 0) AS cumulative_unlocks,
    m.circulating_supply_native as circulating_supply_native
FROM injective.prod_core.ez_metrics m
LEFT JOIN cumulative_rewards cr
  ON m.date = cr.date
LEFT JOIN vesting_data v
  ON m.date = v.date
LEFT JOIN foundation_balance_filled f
  ON m.date = f.date
ORDER BY m.date