{{ config(materialized="table") }}

WITH claimed AS (
  SELECT
    'arbitrum' AS chain,
    BLOCK_TIMESTAMP::DATE AS claim_date,
    SUM(
      TRY_CAST(PARSE_JSON(FULL_DECODED_LOG):data[1]:value::STRING AS DECIMAL(38, 0))
    ) / 1e18 AS total_esgmx_claimed
  FROM arbitrum_flipside.core.ez_decoded_event_logs
  WHERE CONTRACT_ADDRESS = LOWER('0xa75287d2f8b217273e7fcd7e86ef07d33972042e')
    AND EVENT_NAME = 'Claim'
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'avalanche' AS chain,
    BLOCK_TIMESTAMP::DATE AS claim_date,
    SUM(
      TRY_CAST(PARSE_JSON(FULL_DECODED_LOG):data[1]:value::STRING AS DECIMAL(38, 0))
    ) / 1e18 AS total_esgmx_claimed
  FROM avalanche_flipside.core.ez_decoded_event_logs
  WHERE CONTRACT_ADDRESS = LOWER('0x62331a7bd1dfb3a7642b7db50b5509e57ca3154a')
    AND EVENT_NAME = 'Claim'
  GROUP BY 1, 2
),

arb_transfers AS (
  SELECT
    'arbitrum' AS chain,
    DATE_TRUNC('day', block_timestamp) AS claim_date,
    SUM(decoded_log:"value"::FLOAT / 1e18) AS arb_amount
  FROM arbitrum_flipside.core.ez_decoded_event_logs
  WHERE 
    CONTRACT_ADDRESS = LOWER('0x912ce59144191c1204e64559fe8253a0e49e6548')
    AND ORIGIN_TO_ADDRESS = LOWER('0x5384e6cad96b2877b5b3337a277577053bd1941d')
    AND EVENT_NAME = 'Transfer'
  GROUP BY 1, 2
),

gmx_prices AS (
  SELECT
    'arbitrum' AS chain,
    DATE_TRUNC('day', hour) AS price_date,
    AVG(price) AS gmx_price
  FROM arbitrum_flipside.price.ez_prices_hourly
  WHERE TOKEN_ADDRESS = LOWER('0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a')
  GROUP BY 1, 2

  UNION ALL

  SELECT
    'avalanche' AS chain,
    DATE_TRUNC('day', hour) AS price_date,
    AVG(price) AS gmx_price
  FROM avalanche_flipside.price.ez_prices_hourly
  WHERE TOKEN_ADDRESS = LOWER('0x62edc0692bd897d2295872a9ffcac5425011c661')
  GROUP BY 1, 2
),

arb_prices AS (
  SELECT
    'arbitrum' AS chain,
    DATE_TRUNC('day', hour) AS price_date,
    AVG(price) AS arb_price
  FROM arbitrum_flipside.price.ez_prices_hourly
  WHERE TOKEN_ADDRESS = LOWER('0x912ce59144191c1204e64559fe8253a0e49e6548')
  GROUP BY 1, 2
),

combined_rewards AS (
  SELECT
    c.chain,
    c.claim_date,
    ROUND(c.total_esgmx_claimed, 6) AS esgmx_claimed,
    0.0 AS arb_transferred
  FROM claimed c

  UNION ALL

  SELECT
    t.chain,
    t.claim_date,
    0.0 AS esgmx_claimed,
    ROUND(t.arb_amount, 6) AS arb_transferred
  FROM arb_transfers t
),

final_rewards AS (
  SELECT
    r.chain,
    r.claim_date,
    SUM(r.esgmx_claimed) AS total_esgmx_claimed,
    SUM(r.arb_transferred) AS total_arb_transferred,
    MAX(g.gmx_price) AS avg_gmx_price,
    MAX(a.arb_price) AS avg_arb_price
  FROM combined_rewards r
  LEFT JOIN gmx_prices g ON r.chain = g.chain AND r.claim_date = g.price_date
  LEFT JOIN arb_prices a ON r.chain = a.chain AND r.claim_date = a.price_date
  GROUP BY r.chain, r.claim_date
)

SELECT
  chain,
  claim_date,
  ROUND(total_esgmx_claimed, 6) AS total_esgmx_claimed,
  ROUND(total_arb_transferred, 6) AS total_arb_transferred,
  ROUND(avg_gmx_price, 4) AS avg_gmx_price,
  ROUND(avg_arb_price, 4) AS avg_arb_price,
  ROUND(COALESCE(total_esgmx_claimed, 0) * COALESCE(avg_gmx_price, 0) + COALESCE(total_arb_transferred, 0) * COALESCE(avg_arb_price, 0), 2) AS token_incentive_usd,
  ROUND(SUM(
    COALESCE(total_esgmx_claimed, 0) * COALESCE(avg_gmx_price, 0) + COALESCE(total_arb_transferred, 0) * COALESCE(avg_arb_price, 0)
  ) OVER (PARTITION BY chain ORDER BY claim_date), 2) AS cumulative_token_incentive_usd
FROM final_rewards
ORDER BY chain, claim_date

