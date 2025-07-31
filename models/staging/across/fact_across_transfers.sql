{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}

select
    'v1' as version,
    contract_address,
    block_timestamp,
    tx_hash,
    event_index,
    amount,
    depositor,
    recipient,
    destination_chain_id,
    destination_token,
    origin_chain_id,
    realized_lp_fee_pct,
    relayer_fee_pct,
    null as destination_token_symbol,
    null as input_amount,
    null as input_token
from {{ ref("fact_across_v1_transfers") }}
where origin_chain_id != 1919191  -- a bug
union all
SELECT
    'v2+v3' as version
    , src_messaging_contract_address as contract_address
    , coalesce(src_block_timestamp, dst_block_timestamp) as block_timestamp
    , src_tx_hash as tx_hash
    , src_event_index as event_index
    , coalesce(amount_received_native, amount_sent_native) as amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , null as realized_lp_fee_pct
    , null as relayer_fee_pct
    , dst_symbol as destination_token_symbol
    , amount_sent_native as input_amount
    , origin_token as input_token
from {{ref('fact_across_complete_transfers')}}
where coalesce(src_block_timestamp, dst_block_timestamp) is not null
