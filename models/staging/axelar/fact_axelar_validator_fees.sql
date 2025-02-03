{{
    config(
        materialized="table",
        snowflake_warehouse="AXELAR"
    )
}}

with prices as(
  SELECT date(hour) as date, avg(price) as price
  FROM ethereum_flipside.price.ez_prices_hourly
  where NAME = 'Axelar'
  group by 1
)
SELECT
  DATE_TRUNC('DAY', block_timestamp) as date,
  SUM(fee / 1e6) as validator_fees_native,
  COALESCE(SUM(fee / 1e6 * p.price),0) as validator_fees,
FROM axelar_flipside.core.fact_transactions  t
left join prices p on p.date = DATE_TRUNC('DAY', t.block_timestamp)
GROUP BY 1
ORDER BY 1 DESC