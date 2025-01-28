{{
    config (
        materialized="table",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="raw",
        alias="ez_complete_bridge_transfers",
        unique_key=['tx_hash', 'chain', 'event_index'],
    )
}}


SELECT    
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , src_amount
    , src_chain
    , origin_chain_id
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
    , dst_message
    , dst_chain
    , deposit_id
    , protocol_fee
    , bridge_message_app
    , version
FROM 
    {{ ref('fact_across_complete_transfers') }}
WHERE 
    src_block_timestamp <= to_date(sysdate())
