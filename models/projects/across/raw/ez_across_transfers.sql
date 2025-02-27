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
    , amount_sent as src_amount
    , src_chain
    , src_ids.id as origin_chain_id
    , src_token_address as origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received as dst_amount
    , src_address as depositor
    , dst_address as recipient
    , dst_ids.id as destination_chain_id
    , dst_token_address as destination_token
    , null as dst_message
    , dst_chain
    , null as deposit_id
    , fees_native as protocol_fee
    , 'OFT' as bridge_message_app
    , 2 as version
    , 'stargate' as app
FROM 
    {{ ref("fact_stargate_v2_transfers") }} as stargate
left join {{ ref('dim_chain_ids') }} as src_ids on stargate.src_chain = src_ids.chain
left join {{ ref('dim_chain_ids') }} as dst_ids on stargate.dst_chain = dst_ids.chain

union all

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
    , 'across' as app
FROM 
    {{ ref('fact_across_complete_transfers') }}
WHERE 
    (src_block_timestamp <= to_date(sysdate()) or src_block_timestamp is null)
    and (dst_block_timestamp <= to_date(sysdate()) or dst_block_timestamp is null)
