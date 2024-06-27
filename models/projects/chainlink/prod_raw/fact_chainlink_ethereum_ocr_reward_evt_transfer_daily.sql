{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ocr_reward_evt_transfer_daily",
    )
}}


select
  'ethereum' as chain
  , evt_block_time::date as date_start
  , max(cast(date_trunc('month', evt_block_time) as date)) as date_month
  , ocr_reward_evt_transfer.admin_address as admin_address
  , max(ocr_reward_evt_transfer.operator_name) as operator_name
  , sum(token_value) as token_amount
from
  {{ref('fact_chainlink_ethereum_ocr_reward_evt_transfer')}} ocr_reward_evt_transfer
  left join {{ ref('fact_chainlink_ethereum_ocr_operator_admin_meta') }} ocr_operator_admin_meta on lower(ocr_operator_admin_meta.admin_address) = lower(ocr_reward_evt_transfer.admin_address)
group by
  2, 4
order by
  2, 4