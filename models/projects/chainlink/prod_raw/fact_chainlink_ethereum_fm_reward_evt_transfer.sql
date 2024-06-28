{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_fm_reward_evt_transfer"
    )
}}

select
  'ethereum' as chain
  , to_address as admin_address
  , MAX(operator_name) as operator_name
  , MAX(reward_evt_transfer.block_timestamp) as evt_block_time
  , MAX(amount) as token_value
from ethereum_flipside.core.ez_token_transfers reward_evt_transfer
    inner join {{ ref('dim_chainlink_ethereum_price_feeds_oracle_addresses') }} price_feeds ON lower(price_feeds.aggregator_address) = lower(reward_evt_transfer.from_address)
    left join {{ ref('dim_chainlink_ethereum_ocr_operator_admin_meta') }} fm_operator_admin_meta ON lower(fm_operator_admin_meta.admin_address) = lower(reward_evt_transfer.to_address)
group by
  tx_hash
  , event_index
  , to_address