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
    0.0 as foundation_wallet_balance,
    100000000 + COALESCE(cr.cumulative_rewards, 0) AS issued_supply,
    COALESCE(v.remaining_vesting_balance, 0) AS cumulative_unlocks,
    m.circulating_supply_native as circulating_supply_native
FROM injective.prod_core.ez_metrics m
LEFT JOIN cumulative_rewards cr
  ON m.date = cr.date
LEFT JOIN vesting_data v
  ON m.date = v.date
ORDER BY m.date