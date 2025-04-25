{{
    config (
        materialized="table",
        snowflake_warehouse="BRIDGE_FLOWS",
        database="bridge_flows",
        schema="core",
        alias="ez_flows",
    )
}}


SELECT
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , amount_sent_native as src_amount
    , amount_sent_native
    , amount_sent_adjusted
    , amount_sent
    , src_decimals
    , src_symbol
    , src_chain
    , src_ids.id as origin_chain_id
    , src_token_address as origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received_native as dst_amount
    , amount_received_native
    , amount_received_adjusted
    , amount_received
    , dst_decimals
    , dst_symbol
    , src_address as depositor
    , dst_address as recipient
    , dst_ids.id as destination_chain_id
    , dst_token_address as destination_token
    , dst_chain
    , null as token_address
    , null as token_chain
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
    , amount_sent_native as src_amount
    , amount_sent_native
    , amount_sent_adjusted
    , amount_sent
    , src_decimals
    , src_symbol
    , src_chain
    , origin_chain_id
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received_native as dst_amount
    , amount_received_native
    , amount_received_adjusted
    , amount_received
    , dst_decimals
    , dst_symbol
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , dst_chain
    , null as token_address
    , null as token_chain
    , protocol_fee
    , bridge_message_app
    , version
    , 'across' as app
FROM 
    {{ ref('fact_across_complete_transfers') }}
WHERE 
    (src_block_timestamp <= to_date(sysdate()) or src_block_timestamp is null)
    and (dst_block_timestamp <= to_date(sysdate()) or dst_block_timestamp is null)

union all

SELECT
    null as src_messaging_contract_address
    , src_timestamp as src_block_timestamp
    , src_tx_hash
    , null as src_event_index
    , amount_native as src_amount
    , amount_native as amount_sent_native
    , amount_adjusted as amount_sent_adjusted
    , amount as amount_sent
    , decimals as src_decimals
    , symbol as src_symbol
    , source_chain as src_chain
    , null as origin_chain_id
    , null as origin_token
    , null as dst_messaging_contract_address
    , dst_timestamp as dst_block_timestamp
    , dst_tx_hash
    , null as dst_event_index
    , amount_native as dst_amount
    , amount_native as amount_received_native
    , amount_adjusted as amount_received_adjusted
    , amount as amount_received
    , decimals as dst_decimals
    , symbol as dst_symbol
    , from_address as depositor
    , to_address as recipient
    , null as destination_chain_id
    , null as destination_token
    , destination_chain as dst_chain
    , token_address
    , token_chain
    , fee as protocol_fee
    , null as bridge_message_app
    , null as version
    , 'wormhole' as app
FROM 
    {{ ref('fact_wormhole_operations_with_price') }}
WHERE 
    (src_block_timestamp <= to_date(sysdate()) or src_block_timestamp is null)
    and (dst_block_timestamp <= to_date(sysdate()) or dst_block_timestamp is null)
    and (source_chain is not null and destination_chain is not null)
union all
SELECT
    null as src_messaging_contract_address
    , src_timestamp as src_block_timestamp
    , source_tx_hash as src_tx_hash
    , null as src_event_index
    , amount_sent_native as src_amount
    , amount_sent_native
    , amount_sent_adjusted
    , amount_sent
    , source_token_decimals as src_decimals
    , source_token_symbol as src_symbol
    , source_chain as src_chain
    , null as origin_chain_id
    , source_token_address as origin_token
    , null as dst_messaging_contract_address
    , dst_block_timestamp
    , null as dst_tx_hash
    , null as dst_event_index
    , amount_received_native as dst_amount
    , amount_received_native
    , amount_received_adjusted
    , amount_received
    , destination_token_decimals as dst_decimals
    , destination_token_symbol as dst_symbol
    , depositor
    , recipient
    , null as destination_chain_id
    , destination_token_address as destination_token
    , destination_chain as dst_chain
    , null as token_address
    , null as token_chain
    , null as protocol_fee
    , null as bridge_message_app
    , null as version
    , 'debridge' as app
FROM 
    {{ref('fact_debridge_transfers_with_price_and_metadata')}}
WHERE
    src_timestamp <= to_date(sysdate())
    and (source_chain is not null and destination_chain is not null)
