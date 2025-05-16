{{ config(materialized="table") }}

WITH closure_fees AS (
  -- Funding/rollover fees are handled as aggregates on closures and not continuously
  SELECT
    DECODED_LOG:tradeId AS trade_id,
    COALESCE(MAX(CASE WHEN EVENT_NAME = 'FeesCharged' THEN DECODED_LOG:rolloverFees / 1e6 ELSE 0 END), 0) AS rollover_fees,
    COALESCE(MAX(CASE WHEN EVENT_NAME = 'FeesCharged' THEN DECODED_LOG:fundingFees / 1e6 ELSE 0 END), 0) AS funding_fees,
    COALESCE(MAX(CASE WHEN EVENT_NAME = 'VaultLiqFeeCharged' THEN DECODED_LOG:amount / 1e6 ELSE 0 END), 0) AS vault_liq_fees
  FROM
    arbitrum_flipside.core.ez_decoded_event_logs
  WHERE
    ((EVENT_NAME = 'FeesCharged' AND contract_address = LOWER('0x3890243a8fc091c626ed26c087a028b46bc9d66c'))
     OR (EVENT_NAME = 'VaultLiqFeeCharged' AND contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')))
  GROUP BY 1
),

trade_closures AS (
  -- First part: Decoded event logs for standard closures
  SELECT
    block_timestamp,
    tx_hash,
    'close' AS trade_type,
    CASE
      WHEN EVENT_NAME = 'MarketCloseExecuted' THEN 'market'
      WHEN DECODED_LOG:orderType = 0 THEN 'tp'
      WHEN DECODED_LOG:orderType = 1 THEN 'sl'
      WHEN DECODED_LOG:orderType = 2 THEN 'liq'
      WHEN DECODED_LOG:orderType = 3 THEN 'limit'
      ELSE NULL
    END AS order_type,
    TO_NUMERIC(DECODED_LOG:"tradeId") AS trade_id,
    DECODED_LOG:percentProfit / 1e6 AS profit_percent,
    DECODED_LOG:price / 1e18 AS close_price,
    DECODED_LOG:priceImpactP / 1e18 AS closing_price_impact_perc,
    DECODED_LOG:usdcSentToTrader / 1e6 AS usdc_sent,
    TO_NUMERIC(DECODED_LOG:orderId) AS order_id,
    rollover_fees,
    funding_fees,
    vault_liq_fees
  FROM
    arbitrum_flipside.core.ez_decoded_event_logs l
    LEFT JOIN closure_fees f ON l.DECODED_LOG:"tradeId" = f.trade_id
  WHERE
    contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')
    AND TOPIC_0 IN (
      LOWER('0x6d2428396742e21de629bc9398950301d56c1aa493d8de859c4ee751aa02a9b6'), -- LimitCloseExecuted
      LOWER('0x5a988d8359c57f866538ea68df714da9c916a4903309f854821b8a8cb3f376e1')  -- MarketCloseExecuted 
    )
    AND tx_succeeded
  
  UNION ALL
  
  -- Second part: MarketCloseExecutedV2 from raw logs
  SELECT
    block_timestamp,
    tx_hash,
    'close' AS trade_type,
    'market' AS order_type,
    TO_NUMERIC(hex_to_int(TOPIC_2)) AS trade_id,
    TRY_TO_NUMERIC(hex_to_int_with_encoding('s2c', segmented_data[2])) / 1e6 AS profit_percent,
    TRY_TO_NUMERIC(hex_to_int(segmented_data[0])) / 1e18 AS close_price,
    TRY_TO_NUMERIC(hex_to_int(segmented_data[1])) / 1e18 AS closing_price_impact_perc,
    TRY_TO_NUMERIC(hex_to_int(segmented_data[3])) / 1e6 AS usdc_sent,
    TO_NUMERIC(hex_to_int(TOPIC_1)) AS order_id,
    rollover_fees,
    funding_fees,
    vault_liq_fees
  FROM (
    SELECT
      block_timestamp,
      tx_hash,
      TOPIC_1,
      TOPIC_2,
      REGEXP_SUBSTR_ALL(SUBSTR(data, 3), '.{64}') AS segmented_data,
      f.rollover_fees,
      f.funding_fees,
      f.vault_liq_fees
    FROM
      arbitrum_flipside.core.fact_event_logs l
      LEFT JOIN closure_fees f ON TO_NUMERIC(hex_to_int(TOPIC_2)) = f.trade_id
    WHERE
      contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')
      AND TOPIC_0 = LOWER('0xcaa9acf31fbbd991f267d1fe36d806a81db477c3ad5df64ed81b5155b960e8da') -- MarketCloseExecutedV2
      AND tx_succeeded
  )
)

SELECT
  block_timestamp,
  tx_hash,
  trade_type,
  order_type,
  trade_id,
  profit_percent,
  close_price,
  closing_price_impact_perc,
  usdc_sent,
  order_id,
  rollover_fees,
  funding_fees,
  vault_liq_fees
FROM
  trade_closures