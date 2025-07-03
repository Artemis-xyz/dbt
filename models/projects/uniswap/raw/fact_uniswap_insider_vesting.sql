{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
    )
}}

WITH config AS (
  SELECT 
    TO_DATE('2020-09-17') AS start_date,
    1460 AS vesting_days, -- 4 years
    212.66 * 1e6 AS team_total,
    180.44 * 1e6  AS investor_total,
    6.9 * 1e6 AS advisor_total
),

calendar AS (
  SELECT 
    DATEADD(DAY, SEQ4(), c.start_date) AS vesting_date
  FROM TABLE(GENERATOR(ROWCOUNT => 1460)) -- 4 years of days
  CROSS JOIN config c
),

daily_vesting AS (
  SELECT 
    vesting_date,
    team_total / vesting_days AS team_vested_daily,
    investor_total / vesting_days AS investor_vested_daily,
    advisor_total / vesting_days AS advisor_vested_daily
  FROM calendar
  CROSS JOIN config
)
SELECT
    vesting_date as date,
    team_vested_daily,
    SUM(team_vested_daily) OVER (ORDER BY vesting_date) AS team_cumulative,
    investor_vested_daily,
    SUM(investor_vested_daily) OVER (ORDER BY vesting_date) AS investor_cumulative,
    advisor_vested_daily,
    SUM(advisor_vested_daily) OVER (ORDER BY vesting_date) AS advisor_cumulative,
    SUM(team_vested_daily + investor_vested_daily + advisor_vested_daily) OVER (ORDER BY vesting_date) AS total_insider_vested
FROM daily_vesting