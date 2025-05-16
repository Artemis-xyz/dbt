{{ config(materialized="table") }}

with base as (
  SELECT 
    o.*
    , CASE 
        WHEN c.trade_id IS NULL THEN ARRAY_CONSTRUCT(o.block_timestamp) 
        ELSE ARRAY_CONSTRUCT(o.block_timestamp, c.block_timestamp) 
      END as timestamp_arr
    , profit_percent
    , close_price
    , closing_price_impact_perc
    , usdc_sent
    , order_id
    , rollover_fees
    , funding_fees
    , vault_liq_fees
  from {{ ref("fact_ostium_trades_open") }} o
  LEFT JOIN {{ ref("fact_ostium_trades_closures") }} c ON o.trade_id = c.trade_id
)

, refine as (
  SELECT 
    *
    , TO_TIMESTAMP_NTZ(VALUE) as trade_timestamp
  from base, lateral flatten(input=>timestamp_arr)
)

SELECT
  trade_timestamp::date as date
  , SUM(CASE 
      WHEN index=0 THEN volume  -- index=0 open trade, timestamp_arr[0]
      ELSE coalesce(latest_leverage, leverage) * (collateral + coalesce(collateral_delta,0))
      END
    ) as volume_usd
  , SUM(
    CASE 
      WHEN index=0 THEN dev_fee + oracle_fee + vault_open_fee
      ELSE funding_fees + rollover_fees + vault_liq_fees
    END
  ) as total_fees
  , COUNT(DISTINCT trade_id) as trades
  , COUNT(DISTINCT trader) as traders
  , COUNT(DISTINCT market_pair) as markets
from refine 
GROUP BY 1
