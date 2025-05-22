{{ config(materialized="table") }}

with opening_fees as (
  SELECT
    decoded_log:tradeId as trade_id,
    COALESCE(MAX(CASE WHEN event_name = 'DevFeeCharged' THEN decoded_log:amount / 1e6 ELSE 0 END),0) as dev_fee,
    COALESCE(MAX(CASE WHEN event_name = 'VaultOpeningFeeCharged' THEN DECODED_LOG:amount / 1e6 ELSE 0 END), 0) AS vault_open_fee,
    COALESCE(MAX(CASE WHEN event_name = 'OracleFeeCharged' THEN DECODED_LOG:amount / 1e6 ELSE 0 END), 0) AS oracle_fee
  FROM
    arbitrum_flipside.core.ez_decoded_event_logs
  WHERE
    contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')
    AND event_name IN ('DevFeeCharged', 'VaultOpeningFeeCharged', 'OracleFeeCharged')
  GROUP BY 1
) --select * from opening_fees limit 10;
, async_collat_actions as (
  with base as (
    -- First query for TopUpCollateralExecuted events
    SELECT
      block_timestamp,
      tx_hash,
      'TopUpCollateralExecuted' AS EVENT_NAME,
      TO_NUMERIC(DECODED_LOG:tradeId) AS trade_id,
      TO_NUMERIC(DECODED_LOG:pairIndex) AS pair_index,
      DECODED_LOG:topUpAmount / 1e6 AS collateral,
      DECODED_LOG:newLeverage / 1e2 AS leverage,
      NULL AS tp,
      NULL AS sl
    FROM
      arbitrum_flipside.core.ez_decoded_event_logs
    WHERE
      EVENT_NAME = 'TopUpCollateralExecuted'
      AND contract_address = LOWER('0x6d0ba1f9996dbd8885827e1b2e8f6593e7702411')
      AND tx_succeeded
      
    UNION ALL

    -- Second query for RemoveCollateralExecuted events
    SELECT
      block_timestamp,
      tx_hash,
      'RemoveCollateralExecuted' AS event_name,
      hex_to_int(TOPIC_2) AS trade_id,
      TO_NUMERIC(hex_to_int(segmented_data[0])) AS pair_index,
      TO_NUMERIC(hex_to_int(segmented_data[1])) / 1e6 AS collateral,
      TO_NUMERIC(hex_to_int(segmented_data[2])) / 1e2 AS leverage,
      TO_NUMERIC(hex_to_int(segmented_data[3])) / 1e18 AS tp,
      TO_NUMERIC(hex_to_int(segmented_data[4])) / 1e18 AS sl
    FROM (
      SELECT
        block_timestamp,
        tx_hash,
        TOPIC_2,
        REGEXP_SUBSTR_ALL(SUBSTR(data, 3), '.{64}') AS segmented_data
      FROM
        arbitrum_flipside.core.fact_event_logs
      WHERE
        contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')
        AND TOPIC_0 = LOWER('0xd182bace90998b7a07a54165b7beb87743756042148ff3dceb8181dd446533f4')
        AND tx_succeeded
    )
  )
  SELECT
    b.trade_id,
    SUM(CASE WHEN event_name = 'TopUpCollateralExecuted' THEN b.collateral ELSE b.collateral * -1 END) AS collateral_delta,
    MAX(l.leverage) AS latest_leverage,
    MAX(l.tp) AS latest_tp,
    MAX(l.sl) AS latest_sl,
    MAX(l.tx_hash) AS sample1
  FROM
    base b
    LEFT JOIN (
      SELECT
        trade_id,
        tx_hash,
        leverage,
        tp,
        sl,
        ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY block_timestamp DESC) AS rank
      FROM
        base 
      QUALIFY rank = 1
    ) l ON b.trade_id = l.trade_id
  GROUP BY 1
)
, trade_opens AS (
  SELECT
    l.block_timestamp,
    tx_hash,
    DECODED_LOG:"t":index AS trade_index,
    'open' AS trade_type,
    CASE WHEN EVENT_NAME = 'LimitOpenExecuted' THEN 'limit' ELSE 'market' END AS order_type,
    DECODED_LOG:"orderId" AS trade_id,
    tuple:"pairIndex" AS pair_index,
    market_pair,
    CASE WHEN tuple:buy = TRUE THEN 'buy / long' ELSE 'sell / short' END AS side,
    tuple:collateral / 1e6 AS collateral,
    tuple:leverage / 1e2 AS leverage,
    tuple:"openPrice" / 1e18 AS open_price,
    tuple:sl / 1e18 AS stop_loss,
    tuple:tp / 1e18 AS take_profit,
    tuple:trader AS trader,
    DECODED_LOG:tradeNotional AS notional_raw,
    (tuple:collateral / 1e6) * (tuple:leverage / 1e2) AS volume,
    DECODED_LOG:priceImpactP / 1e18 AS price_impact_percent,
    f.dev_fee,
    f.oracle_fee,
    f.vault_open_fee,
    a.collateral_delta,
    a.latest_leverage,
    a.latest_tp,
    a.latest_sl
  FROM
    arbitrum_flipside.core.ez_decoded_event_logs l
    CROSS JOIN LATERAL (SELECT DECODED_LOG:"t" AS tuple)
    LEFT JOIN pc_dbt_db.prod.fact_ostium_market_pairs mp ON l.DECODED_LOG:"t":"pairIndex" = mp.pair_index
    LEFT JOIN opening_fees f ON l.DECODED_LOG:"orderId" = f.trade_id
    LEFT JOIN async_collat_actions a ON l.DECODED_LOG:"orderId" = a.trade_id
  WHERE
    contract_address = LOWER('0x7720fc8c8680bf4a1af99d44c6c265a74e9742a9')
    AND TOPIC_0 IN (
      LOWER('0x19c8a7be769082e3461a241a3b6af0adad9302b7c1623ab8adb4d787fd9df67c'), -- LimitOpenExecuted
      LOWER('0xf14b61759b2364f919bb1ce7c68a72e7c012733a5225c194c4f9e8460d9bc0ee')  -- MarketOpenExecuted
    )
    AND tx_succeeded
)

SELECT
  block_timestamp,
  tx_hash,
  trade_index,
  trade_type,
  order_type,
  trade_id,
  pair_index,
  market_pair,
  side,
  collateral,
  leverage,
  open_price,
  stop_loss,
  take_profit,
  trader,
  notional_raw,
  volume,
  price_impact_percent,
  dev_fee,
  oracle_fee,
  vault_open_fee,
  collateral_delta,
  latest_leverage,
  latest_tp,
  latest_sl
FROM
  trade_opens