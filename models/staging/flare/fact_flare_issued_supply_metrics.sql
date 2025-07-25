{{ config(materialized="table") }}

-- Step 1: Compute true cumulative metrics (no lag yet)
WITH raw_base_data AS (
  SELECT 
    date,
    foundation_unlocks_daily,
    team_unlocks_daily,
    total_unlocks_daily,
    burns_daily,

    SUM(foundation_unlocks_daily) OVER (ORDER BY date) AS cum_foundation_unlocks_raw,
    SUM(team_unlocks_daily) OVER (ORDER BY date) AS cum_team_unlocks_raw,
    SUM(total_unlocks_daily) OVER (ORDER BY date) AS cum_total_unlocks_raw,
    SUM(burns_daily) OVER (ORDER BY date) AS cum_burns_raw

  FROM {{ source('MANUAL_STATIC_TABLES', 'flare_daily_supply_data_2') }}
),

-- Step 2: Lag the cumulative metrics by 1 day so that Day 0 = 0
base_data AS (
  SELECT
    date,
    foundation_unlocks_daily,
    team_unlocks_daily,
    total_unlocks_daily,
    burns_daily,

    LAG(cum_foundation_unlocks_raw, 1, 0) OVER (ORDER BY date) AS cumulative_foundation_unlocks,
    LAG(cum_team_unlocks_raw, 1, 0) OVER (ORDER BY date) AS cumulative_team_unlocks,
    LAG(cum_total_unlocks_raw, 1, 0) OVER (ORDER BY date) AS cumulative_unlocks,
    LAG(cum_burns_raw, 1, 0) OVER (ORDER BY date) AS cumulative_burns
  FROM raw_base_data
),

-- Step 3: Inflation model
inflation_model (
  date,
  foundation_unlocks_daily,
  cumulative_foundation_unlocks,
  team_unlocks_daily,
  cumulative_team_unlocks,
  total_unlocks_daily,
  cumulative_unlocks,
  burns_daily,
  cumulative_burns,
  daily_inflation,
  cumulative_inflation,
  circulating_supply
) AS (

  -- Seed row
  SELECT
    date,
    foundation_unlocks_daily,
    cumulative_foundation_unlocks,
    team_unlocks_daily,
    cumulative_team_unlocks,
    total_unlocks_daily,
    cumulative_unlocks,
    burns_daily,
    cumulative_burns,
    CAST(0.0 AS DOUBLE) AS daily_inflation,
    CAST(0.0 AS DOUBLE) AS cumulative_inflation,
    cumulative_unlocks - cumulative_burns AS circulating_supply
  FROM base_data
  WHERE date = (SELECT MIN(date) FROM base_data)

  UNION ALL

  -- Recursive logic
  SELECT
    b.date,
    b.foundation_unlocks_daily,
    b.cumulative_foundation_unlocks,
    b.team_unlocks_daily,
    b.cumulative_team_unlocks,
    b.total_unlocks_daily,
    b.cumulative_unlocks,
    b.burns_daily,
    b.cumulative_burns,

    CASE
      WHEN b.date < '2023-01-10' THEN 0.0
      WHEN b.date < '2023-07-05' THEN im.circulating_supply * 0.10 / 365
      WHEN b.date < '2024-07-05' THEN im.circulating_supply * 0.07 / 365
      ELSE im.circulating_supply * 0.05 / 365
    END AS daily_inflation,

    im.cumulative_inflation +
      CASE
        WHEN b.date < '2023-01-10' THEN 0.0
        WHEN b.date < '2023-07-05' THEN im.circulating_supply * 0.10 / 365
        WHEN b.date < '2024-07-05' THEN im.circulating_supply * 0.07 / 365
        ELSE im.circulating_supply * 0.05 / 365
      END AS cumulative_inflation,

    b.cumulative_unlocks +
      (
        im.cumulative_inflation +
        CASE
          WHEN b.date < '2023-01-10' THEN 0.0
          WHEN b.date < '2023-07-05' THEN im.circulating_supply * 0.10 / 365
          WHEN b.date < '2024-07-05' THEN im.circulating_supply * 0.07 / 365
          ELSE im.circulating_supply * 0.05 / 365
        END
      ) - b.cumulative_burns AS circulating_supply

  FROM inflation_model im
  JOIN base_data b ON b.date = DATEADD(DAY, 1, im.date)
)

-- Final output
SELECT
    date,
    100000000000 AS initial_supply,
    daily_inflation,
    burns_daily,
    total_unlocks_daily,
    cumulative_inflation as inflation,
    cumulative_burns as burns,
    initial_supply + cumulative_inflation as max_supply_to_date,
    initial_supply + cumulative_inflation - burns as total_supply_to_date,
    cumulative_foundation_unlocks,
    CASE
        WHEN 71300000000 - cumulative_foundation_unlocks > 0 THEN 71300000000 - cumulative_foundation_unlocks
        ELSE 0
    END AS foundation_balance,
    total_supply_to_date - burns - foundation_balance as issued_supply,
    cumulative_team_unlocks,
    CASE
        WHEN 28700000000 - cumulative_team_unlocks > 0 THEN 28700000000 - cumulative_team_unlocks
        ELSE 0
    END AS team_vesting_balance,
    cumulative_unlocks,
    circulating_supply,
FROM inflation_model
ORDER BY date
