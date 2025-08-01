{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

WITH RECURSIVE date_range AS (
  SELECT '2024-01-26'::DATE AS date
  UNION ALL
  SELECT DATEADD(day, 1, date)
    FROM date_range
   WHERE date < CURRENT_DATE()
),

-- 2. Daily burn + true end‑of‑day balance in one go
  burn_daily AS (
    SELECT
      date,
      burn_address_balance,
      burn
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS date,
        balance AS burn_address_balance,

        -- total burned that day (only negative deltas)
        SUM(
          CASE WHEN (balance - pre_balance) < 0 THEN (pre_balance - balance) ELSE 0 END
        ) OVER (PARTITION BY CAST(block_timestamp AS DATE)) AS burn,

        -- rank snapshots so the very last one is rn = 1
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM 
        {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint           = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner          = '8gMBNeKwXaoNi9bhbVUWFt4Uc5aobL9PeYMXfYDMePE2'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    ) t
    WHERE rn = 1
  ),

-- 3. Buy‑back wallet: last balance each day, then forward‑fill
  buyback_last_bal AS (
    SELECT dt AS date, balance
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint  = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = '6tZT9AUcQn4iHMH79YZEXSy55kDLQ4VbA3PMtfLVNsFX'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    )
    WHERE rn = 1
  ),
  buyback_daily AS (
    SELECT dr.date,
      LAST_VALUE(blb.balance IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS buyback
    FROM date_range dr
    LEFT JOIN buyback_last_bal blb ON dr.date = blb.date
  ),

-- 4. Foundation wallets – cold & hot (forward‑filled)
  found_cold_last AS (
    SELECT dt AS date, balance
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint  = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = 'EXJHiMkj6NRFDfhWBMKccHNwdSpCT7tdvQeRf87yHm6T'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    )
    WHERE rn = 1
  ),
  found_cold_daily AS (
    SELECT dr.date,
      LAST_VALUE(fcl.balance IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS foundation_cold
    FROM date_range dr
    LEFT JOIN found_cold_last fcl ON dr.date = fcl.date
  ),

  found_hot_last AS (
    SELECT dt AS date, balance
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint  = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = 'FVhQ3QHvXudWSdGix2sdcG47YmrmUxRhf3KCBmiKfekf'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    )
    WHERE rn = 1
  ),
  found_hot_daily AS (
    SELECT dr.date,
      LAST_VALUE(fhl.balance IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS foundation_hot
    FROM date_range dr
    LEFT JOIN found_hot_last fhl ON dr.date = fhl.date
  ),

-- 5. Team wallets – cold & hot (forward‑filled)
  team_cold_last AS (
    SELECT dt AS date, balance
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint  = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = '61aq585V8cR2sZBeawJFt2NPqmN7zDi1sws4KLs5xHXV'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    )
    WHERE rn = 1
  ),
  team_cold_daily AS (
    SELECT dr.date,
      LAST_VALUE(tcl.balance IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS team_cold
    FROM date_range dr
    LEFT JOIN team_cold_last tcl ON dr.date = tcl.date
  ),

  team_hot_last AS (
    SELECT dt AS date, balance
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint  = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = 'CbU4oSFCk8SVgW23NLvb5BwctvWcZZHfxRD6HudP8gAo'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    )
    WHERE rn = 1
  ),
  team_hot_daily AS (
    SELECT dr.date,
      LAST_VALUE(thl.balance IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS team_hot
    FROM date_range dr
    LEFT JOIN team_hot_last thl ON dr.date = thl.date
  ),

-- 5c. Team hot wallet #2 – last snapshot per day
  team_hot2_last AS (
    SELECT dt AS date, balance AS hot_balance_2
    FROM (
      SELECT
        CAST(block_timestamp AS DATE) AS dt,
        balance,
        ROW_NUMBER() OVER (
          PARTITION BY CAST(block_timestamp AS DATE)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM {{ source('SOLANA_FLIPSIDE', 'fact_token_balances')}}
      WHERE mint = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
        AND owner = 'Any5gL74oUQy9Psb3goym9STaYMau4CMrTkryS37T7iz'
        AND block_timestamp >= '2024-01-26'::TIMESTAMP
    ) t
    WHERE rn = 1
  ),
  team_hot2_daily AS (
    SELECT dr.date,
      LAST_VALUE(th2l.hot_balance_2 IGNORE NULLS) OVER (
        ORDER BY dr.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS team_hot_balance_2
    FROM date_range dr
    LEFT JOIN team_hot2_last th2l ON dr.date = th2l.date
  ),

-- 6. Combine all series & derive cumulative and supply metrics
combined AS (
  SELECT
    dr.date,

    -- raw activity
    COALESCE(bd.burn, 0)           AS burn,
    COALESCE(bbd.buyback, 0)       AS buyback,

    -- foundation stock
    COALESCE(fcd.foundation_cold, 0) AS foundation_cold,
    COALESCE(fhd.foundation_hot,   0) AS foundation_hot,

    -- team stock
    COALESCE(tcd.team_cold,       0) AS team_cold,
    COALESCE(thd.team_hot,        0) AS team_hot,
    COALESCE(th2.team_hot_balance_2, 0) AS team_hot_balance_2,

    -- burn address balance (now forward‑filled)
    COALESCE(
      LAST_VALUE(bd.burn_address_balance IGNORE NULLS)
        OVER (
          ORDER BY dr.date
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
      0
    ) AS burn_balance

  FROM date_range dr
  LEFT JOIN burn_daily        bd  ON dr.date = bd.date
  LEFT JOIN buyback_daily     bbd ON dr.date = bbd.date
  LEFT JOIN found_cold_daily  fcd ON dr.date = fcd.date
  LEFT JOIN found_hot_daily   fhd ON dr.date = fhd.date
  LEFT JOIN team_cold_daily   tcd ON dr.date = tcd.date
  LEFT JOIN team_hot_daily    thd ON dr.date = thd.date
  LEFT JOIN team_hot2_daily   th2 ON dr.date = th2.date
  WHERE dr.date <= CURRENT_DATE()
)

SELECT
  date,
  burn,
  buyback,
  foundation_cold,
  foundation_hot,
  team_cold,
  team_hot,
  team_hot_balance_2,
  burn_balance,
  SUM(burn) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_burn,
  10000000000 AS max_supply,
  10000000000 - SUM(burn) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS total_supply,
  10000000000
    - SUM(burn) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    - (foundation_cold + foundation_hot + burn_balance + buyback) AS issued_supply,
  10000000000
    - SUM(burn) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )
    - (foundation_cold + foundation_hot + team_cold + team_hot + team_hot_balance_2 + burn_balance + buyback) AS circulating_supply
FROM combined
ORDER BY date


