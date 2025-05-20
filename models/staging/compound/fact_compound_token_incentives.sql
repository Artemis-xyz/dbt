{{
    config(
        materialized = 'table',
    )
}}

WITH v2_rewards_raw AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(TRY_CAST(decoded_log:"compDelta"::string AS FLOAT)) / 1e18 AS total_comp
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE
    contract_address = LOWER('0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b')
    AND event_name IN ('DistributedSupplierComp', 'DistributedBorrowerComp')
  GROUP BY 1
),

v2_prices AS (
  SELECT
    DATE_TRUNC('day', hour) AS day,
    AVG(price) AS comp_price
  FROM ethereum_flipside.price.ez_prices_hourly
  WHERE token_address = LOWER('0xc00e94cb662c3520282e6f5717214004a7f26888')
  GROUP BY 1
),

v2_usd AS (
  SELECT
    r.day,
    r.total_comp,
    p.comp_price,
    r.total_comp * p.comp_price AS usd_value
  FROM v2_rewards_raw r
  JOIN v2_prices p ON r.day = p.day
),

v2_with_median AS (
  SELECT
    day,
    usd_value,
    DATE_TRUNC('month', day) AS month,
    MEDIAN(usd_value) OVER (PARTITION BY DATE_TRUNC('month', day)) AS monthly_median
  FROM v2_usd
),

v2_final AS (
  SELECT
    day,
    CASE 
      WHEN usd_value > 5000000 THEN monthly_median
      ELSE usd_value
    END AS corrected_usd_value
  FROM v2_with_median
),

v3_rewards_transfers AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(TRY_CAST(decoded_log:"amount"::string AS FLOAT)) / 1e18 AS total_comp
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE
    ORIGIN_TO_ADDRESS = LOWER('0x1b0e765f6224c21223aea2af16c1c46e38885a40')
    AND CONTRACT_ADDRESS = LOWER('0xc00e94cb662c3520282e6f5717214004a7f26888')
    AND event_name = 'Transfer'
    AND decoded_log:"from" = LOWER('0x1b0e765f6224c21223aea2af16c1c46e38885a40')
  GROUP BY 1
),

v3_rewards_claimed AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(TRY_CAST(decoded_log:"amount"::string AS FLOAT)) / 1e18 AS total_comp
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE
    contract_address = LOWER('0x1b0e765f6224c21223aea2af16c1c46e38885a40')
    AND event_name = 'RewardClaimed'
    AND decoded_log:"token" = LOWER('0xc00e94cb662c3520282e6f5717214004a7f26888')
  GROUP BY 1
),

v3_combined AS (
  SELECT * FROM v3_rewards_transfers
  UNION ALL
  SELECT * FROM v3_rewards_claimed
),

v3_daily_rewards AS (
  SELECT
    day,
    SUM(total_comp) AS total_comp
  FROM v3_combined
  GROUP BY 1
),

v3_prices AS (
  SELECT
    DATE_TRUNC('day', hour) AS day,
    AVG(price) AS comp_price
  FROM ethereum_flipside.price.ez_prices_hourly
  WHERE token_address = LOWER('0xc00e94cb662c3520282e6f5717214004a7f26888')
  GROUP BY 1
),

v3_final AS (
  SELECT
    r.day,
    r.total_comp * p.comp_price AS corrected_usd_value
  FROM v3_daily_rewards r
  JOIN v3_prices p ON r.day = p.day
),

combined_final AS (
  SELECT * FROM v2_final
  UNION ALL
  SELECT * FROM v3_final
)

SELECT
  day,
  SUM(corrected_usd_value) AS total_usd_value
FROM combined_final
GROUP BY 1
ORDER BY 1