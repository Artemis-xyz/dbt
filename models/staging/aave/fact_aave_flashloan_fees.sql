{{
    config(
        materialized="table",
    )
}}

WITH premium_updates AS (
  SELECT
    block_timestamp::DATE AS update_date,
    decoded_log:newFlashloanPremiumToProtocol::INT AS protocol_bps
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE event_name = 'FlashloanPremiumToProtocolUpdated'
    AND contract_address = lower('0x64b761d848206f447fe2dd461b0c635ec39ebb27')
),

flashloan_fees AS (
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_ARBITRUM_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V2_AVALANCHE_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_AVALANCHE_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_BASE_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V2_ETHEREUM_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_ETHEREUM_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_GNOSIS_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_OPTIMISM_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V2_POLYGON_FLASHLOAN_FEES
    UNION ALL
    SELECT * FROM AAVE.PROD_RAW.FACT_V3_POLYGON_FLASHLOAN_FEES
),

flashloan_with_bps AS (
  SELECT
    f.date,
    f.amount_usd,
    coalesce(protocol_bps, 0) as protocol_bps
  FROM flashloan_fees f
  LEFT JOIN premium_updates p
    ON p.update_date <= f.date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.date ORDER BY p.update_date DESC) = 1
),

aave_flashloan_fees AS (
  SELECT
    date,
    MEDIAN(protocol_bps) as protocol_bps,
    SUM(amount_usd) AS total_flashloan_fees,
    SUM(amount_usd * protocol_bps / 10000.0) AS protocol_revenue,
    SUM(amount_usd * (10000 - protocol_bps) / 10000.0) AS lp_revenue
  FROM flashloan_with_bps
  GROUP BY date
)

SELECT * FROM aave_flashloan_fees
ORDER BY date DESC