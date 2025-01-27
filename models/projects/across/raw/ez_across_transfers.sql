{{config(materialized='table', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS")}}
SELECT    
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
    , bridge_message_app
    , version
FROM 
    {{ ref('fact_across_complete_transfers') }}
