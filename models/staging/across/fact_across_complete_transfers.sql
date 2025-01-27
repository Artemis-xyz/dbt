{{config(materialized='table', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS")}}
select
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , src_amount
    , src_relayer_fee_pct
    , src_chain
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , dst_amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , src_realized_lp_fee_pct as realized_lp_fee_pct
    , dst_relayer_fee_pct
    , dst_message
    , dst_chain
    , deposit_id
    , protocol_fee
    , 'across' as bridge_message_app
    , '3' as version
from {{ ref('fact_across_v3_complete_transfers') }}
union all
select
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , src_amount
    , src_relayer_fee_pct
    , src_chain
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , dst_amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , src_realized_lp_fee_pct as realized_lp_fee_pct
    , dst_relayer_fee_pct
    , dst_message
    , dst_chain
    , deposit_id
    , protocol_fee
    , 'across' as bridge_message_app
    , '2' as version
from {{ ref('fact_across_v2_complete_transfers') }}
