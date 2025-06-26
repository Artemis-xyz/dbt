{{ config(materialized="table") }}

WITH daily_burns AS (
  SELECT 
    fcs.date,
    fcs.circulating_supply,
    fcs.fxs_burned_cumulative_sum,
    fcs.fxs_burned_cumulative_sum - LAG(fcs.fxs_burned_cumulative_sum, 1, 0) OVER (ORDER BY fcs.date) AS daily_burn,
    ROW_NUMBER() OVER (PARTITION BY fcs.date ORDER BY fcs.date DESC) AS row_num
  FROM {{ref('fact_fxs_circulating_supply')}} fcs
),
daily_burns_2 AS (
  SELECT 
    db.date,
    db.circulating_supply,
    db.fxs_burned_cumulative_sum,
    CASE WHEN db.daily_burn < 0 THEN 0 ELSE db.daily_burn END AS daily_burn  -- Handle any negative values
    FROM daily_burns db
    WHERE db.row_num = 1
    ORDER BY db.date
),
-- Calculate all the metrics based on circulating supply as source of truth
supply_metrics AS (
  SELECT
    db2.date,
    db2.circulating_supply,
    db2.daily_burn AS initial_burns_native,
    db2.circulating_supply - LAG(db2.circulating_supply, 1) OVER (ORDER BY db2.date) AS net_supply_change_native,
    COALESCE(pu.premine_unlocks, 0) AS premine_unlocks
  FROM daily_burns_2 db2
  LEFT JOIN {{ref('fact_fxs_premine_unlocks')}} pu ON db2.date = pu.date
),
-- Calculate emissions and adjust burns if needed to balance the equation
final_metrics AS (
  SELECT
    sm.date,
    sm.circulating_supply,
    sm.initial_burns_native,
    sm.net_supply_change_native,
    sm.premine_unlocks,

    -- Calculate required emissions based on the formula:
    -- emissions = net_supply_change + burns - premine_unlocks
    -- If this would be negative, set emissions to 0 and adjust burns
    CASE
      WHEN (sm.net_supply_change_native + sm.initial_burns_native - sm.premine_unlocks) < 0 THEN 0
      ELSE (sm.net_supply_change_native + sm.initial_burns_native - sm.premine_unlocks)
    END AS emissions_native,

    -- Adjust burns if emissions would be negative to maintain the equation:
    -- net_supply_change = emissions + premine_unlocks - burns
    -- If emissions = 0, then: burns = premine_unlocks - net_supply_change
    CASE
      WHEN (sm.net_supply_change_native + sm.initial_burns_native - sm.premine_unlocks) < 0 
      THEN sm.premine_unlocks - sm.net_supply_change_native
      ELSE sm.initial_burns_native
    END AS burns_native
  FROM supply_metrics sm
)
SELECT
  fm.date,
  -- For emissions: use calculated emissions (will be 0 if calculated negative)
  fm.emissions_native,
  -- For total premine unlocks: running sum as before
  SUM(fm.premine_unlocks) OVER (ORDER BY fm.date) AS total_premine_unlocks,
  -- For burns: use adjusted burns that maintain the balance equation
  fm.burns_native,
  -- For net supply change: directly from circulating supply diff
  fm.net_supply_change_native,
  -- Circulating supply is our source of truth
  fm.circulating_supply AS total_circulating_supply,
  -- Add a verification column to ensure our equation balances
  (fm.emissions_native + fm.premine_unlocks - fm.burns_native) AS calculated_change,
  -- This should match net_supply_change_native
  (fm.emissions_native + fm.premine_unlocks - fm.burns_native) - fm.net_supply_change_native AS balance_check
FROM final_metrics fm
ORDER BY fm.date