{{ config(materialized="table", snowflake_warehouse="TRON") }}

-- Step 0: Generate date spine
WITH date_spine AS (
  SELECT
    DATEADD(day, seq4(), DATE '2018-06-25') AS date
  FROM TABLE(GENERATOR(ROWCOUNT => 3000))
  WHERE DATEADD(day, seq4(), DATE '2018-06-25') <= CURRENT_DATE -- 2018rotocol start date
),

-- Step 1: Block issuance logic, will need updating if protocol rewards change in future
block_rewards_classified AS (
  SELECT
    b.timestamp,
    b.number,
    CASE 
      WHEN b.number < 14228706 THEN 32
      WHEN b.number < 59200000 THEN 16
      ELSE 8
    END AS block_rewards,
    CASE 
      WHEN b.number < 14228706 THEN 16
      WHEN b.number < 59200000 THEN 160
      ELSE 128
    END AS voting_rewards
  FROM {{ source("TRON_ALLIUM", "blocks") }} b
),
block_issuance AS (
  SELECT *,
    block_rewards + voting_rewards AS rewards_this_block
  FROM block_rewards_classified
),
cumulative_rewards AS (
  SELECT *,
    SUM(rewards_this_block) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rewards_cumsum
  FROM block_issuance
),
with_genesis_supply AS (
  SELECT *,
    99000000000 + rewards_cumsum AS max_supply_to_date
  FROM cumulative_rewards
),
daily_issued_supply AS (
  SELECT
    DATE_TRUNC('day', timestamp) AS date,
    MAX(max_supply_to_date) AS max_supply_to_date
  FROM with_genesis_supply
  GROUP BY 1
),

-- Step 2: Burned TRX
daily_burned AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS date,
    SUM(amount) AS burned_trx
  FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
  WHERE to_address = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'
  GROUP BY 1
),
cumulative_burned AS (
  SELECT
    ds.date,
    COALESCE(db.burned_trx, 0) AS burned_trx
  FROM date_spine ds
  LEFT JOIN daily_burned db ON ds.date = db.date
),
cumulative_burned_summed AS (
  SELECT
    date,
    burned_trx,
    SUM(burned_trx) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burned_trx
  FROM cumulative_burned
),

-- Step 3: Foundation balance tracking
foundation_distribution AS (
    SELECT DISTINCT to_address
    FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
    WHERE from_address = 'TSnjgPDQfuxx72iaPy82v3T8HrsN4GVJzW'
        AND to_address != 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'

    UNION ALL 
    -- Manually include one known foundation addresses --> not including address 2 (TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb)
    SELECT 'TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9'
    
    --UNION ALL
    --SELECT 'TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb'
),
foundation_daily_net AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date, 
        to_address AS address, 
        SUM(amount) AS net_inflow
    FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
    WHERE to_address IN (SELECT to_address FROM foundation_distribution)
    GROUP BY 1, 2
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date, 
        from_address AS address, 
        -SUM(amount) AS net_outflow
    FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
    WHERE from_address IN (SELECT to_address FROM foundation_distribution)
    GROUP BY 1, 2
),
foundation_net_flow AS (
  SELECT date, SUM(net_inflow) AS daily_net_flow FROM foundation_daily_net GROUP BY 1
),
foundation_balance AS (
  SELECT
    ds.date,
    SUM(COALESCE(f.daily_net_flow, 0)) OVER (
      ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS foundation_wallet_balance
  FROM date_spine ds
  LEFT JOIN foundation_net_flow f ON ds.date = f.date
),

-- Step 4: Team vesting tracking
team_distribution AS (
  SELECT DISTINCT to_address
  FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
  WHERE from_address IN (
    'TMmF9dxMSGc4ez7KszamX892WjxHwPfs8g',
    'TXAxFbydXX5B4r5ZefWQTYBy4nxoQwNfmW',
    'TAvDfzoEVsb9vyDn3gf6n3oGmL84p5KQFF'
  )
    AND to_address != 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb'
),
team_daily_net AS (
  SELECT DATE_TRUNC('day', block_timestamp) AS date, to_address AS address, SUM(amount) AS net_inflow
  FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
  WHERE to_address IN (SELECT to_address FROM team_distribution)
  GROUP BY 1, 2
  UNION ALL
  SELECT DATE_TRUNC('day', block_timestamp) AS date, from_address AS address, -SUM(amount) AS net_outflow
  FROM {{ source("TRON_ALLIUM_ASSETS", "trx_token_transfers") }}
  WHERE from_address IN (SELECT to_address FROM team_distribution)
  GROUP BY 1, 2
),
team_net_flow AS (
  SELECT date, SUM(net_inflow) AS daily_net_flow FROM team_daily_net GROUP BY 1
),
team_balance AS (
  SELECT
    ds.date,
    SUM(COALESCE(t.daily_net_flow, 0)) OVER (
      ORDER BY ds.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS team_vesting_balance
  FROM date_spine ds
  LEFT JOIN team_net_flow t ON ds.date = t.date
)
, coingecko_supply AS (
  SELECT
    date,
    shifted_token_circulating_supply
  FROM {{ ref("fact_coingecko_token_date_adjusted_gold") }}
  WHERE coingecko_id = 'tron'
)

-- Final Step: Combine everything
, final_combined AS (
    SELECT
        ds.date,
        COALESCE(dis.max_supply_to_date, 99000000000) AS max_supply_to_date,
        0 AS uncreated_tokens,
        max_supply_to_date - uncreated_tokens as total_supply,
        COALESCE(cb.cumulative_burned_trx, 0) AS cumulative_burned_trx,
        COALESCE(fb.foundation_wallet_balance, 0) AS foundation_wallet_balance,
        total_supply - cumulative_burned_trx - foundation_wallet_balance AS issued_supply,
        COALESCE(tb.team_vesting_balance, 0) AS team_vesting_balance,
        issued_supply
          - team_vesting_balance AS floating_supply,
        cg.shifted_token_circulating_supply as coingecko_circulating_supply
    FROM date_spine ds
    LEFT JOIN daily_issued_supply dis ON ds.date = dis.date
    LEFT JOIN cumulative_burned_summed cb ON ds.date = cb.date
    LEFT JOIN foundation_balance fb ON ds.date = fb.date
    LEFT JOIN team_balance tb ON ds.date = tb.date
    LEFT JOIN coingecko_supply cg ON ds.date = cg.date
)

-- Output
SELECT *
FROM final_combined
ORDER BY date


