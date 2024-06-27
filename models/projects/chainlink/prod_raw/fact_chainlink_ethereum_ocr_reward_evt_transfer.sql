{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ocr_reward_evt_transfer",
    )
}}


select
    'ethereum' as chain
    , reward_evt_transfer.to_address as admin_address
    , max(operator_name) as operator_name
    , max(reward_evt_transfer.block_timestamp) as evt_block_time
    , max(reward_evt_transfer.amount) as token_value
from ethereum_flipside.core.ez_token_transfers reward_evt_transfer
right join {{ ref('fact_chainlink_ethereum_ocr_reward_transmission_logs') }} ocr_reward_transmission_logs 
    on lower(ocr_reward_transmission_logs.contract_address) = lower(reward_evt_transfer.from_address)
left join {{ ref('fact_chainlink_ethereum_ocr_operator_admin_meta') }} ocr_operator_admin_meta 
    on lower(ocr_operator_admin_meta.admin_address) = lower(reward_evt_transfer.to_address)
where lower(reward_evt_transfer.from_address) in (select lower(contract_address) from {{ ref('fact_chainlink_ethereum_ocr_reward_transmission_logs') }})
GROUP BY
  reward_evt_transfer.tx_hash
  , reward_evt_transfer.event_index
  , reward_evt_transfer.to_address