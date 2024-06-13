{{ config(materialized="table", snowflake_warehouse="GEODNET") }}
with burns as (
  select
    trunc(block_timestamp, 'day') date,
    amount_usd
  from
    polygon_flipside.core.ez_token_transfers
  where
    lower(contract_address) = lower('0xAC0F66379A6d7801D7726d5a943356A172549Adb')
    and lower(to_address) = lower('0x000000000000000000000000000000000000dead')
)
select
  date,
  sum(amount_usd) as revenue,
  sum(amount_usd) / 0.8 as fees,
  'polygon' as chain,
  'geodnet' as protocol
from
  burns
GROUP by
  1
ORDER by
  1 ASC