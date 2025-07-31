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
    , amount_sent as src_amount_usd
    , amount_sent_native::number as src_amount_native
    , amount_sent_adjusted as src_amount_adjusted
    , src_decimals
    , src_symbol
    , src_chain
    , src_ids.id as src_chain_id
    , src_token_address
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received as dst_amount_usd
    , amount_received_native::number as dst_amount_native
    , amount_received_adjusted as dst_amount_adjusted
    , dst_decimals
    , dst_symbol
    , src_address as depositor
    , dst_address as recipient
    , dst_ids.id as dst_chain_id
    , dst_token_address
    , dst_chain
    , null as token_address
    , null as token_chain
    , fees as protocol_fee
    , 'OFT' as bridge_message_app
    , 2 as version
    , 'stargate' as app
    , concat(guid, '|', 'stargate') as unique_id
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
    , amount_sent as src_amount_usd
    , amount_sent_native::number as src_amount_native
    , amount_sent_adjusted as src_amount_adjusted
    , src_decimals
    , src_symbol
    , src_chain
    , origin_chain_id as src_chain_id
    , origin_token as src_token_address
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received as dst_amount_usd
    , amount_received_native::number as dst_amount_native
    , amount_received_adjusted as dst_amount_adjusted
    , dst_decimals
    , dst_symbol
    , depositor
    , recipient
    , destination_chain_id as dst_chain_id
    , destination_token as dst_token_address
    , dst_chain
    , null as token_address
    , null as token_chain
    , protocol_fee
    , bridge_message_app
    , version
    , 'across' as app
    , unique_id
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
    , amount as src_amount_usd
    , amount_native as src_amount_native
    , amount_adjusted as src_amount_adjusted
    , decimals as src_decimals
    , symbol as src_symbol
    , source_chain as src_chain
    , src_chain_id
    , null as src_token_address
    , null as dst_messaging_contract_address
    , dst_timestamp as dst_block_timestamp
    , dst_tx_hash
    , null as dst_event_index
    , amount as dst_amount_usd
    , amount_native as dst_amount_native
    , amount_adjusted as dst_amount_adjusted
    , decimals as dst_decimals
    , symbol as dst_symbol
    , from_address as depositor
    , to_address as recipient
    , dst_chain_id
    , null as dst_token_address
    , destination_chain as dst_chain
    , token_address
    , token_chain
    , fee as protocol_fee
    , null as bridge_message_app
    , null as version
    , 'wormhole' as app
    , unique_id
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
    , amount_sent_adjusted as src_amount_usd
    , amount_sent_native::number as src_amount_native
    , amount_sent_adjusted as src_amount_adjusted
    , source_token_decimals as src_decimals
    , source_token_symbol as src_symbol
    , source_chain as src_chain
    , src_chain_id
    , source_token_address as src_token_address
    , null as dst_messaging_contract_address
    , dst_block_timestamp
    , null as dst_tx_hash
    , null as dst_event_index
    , amount_received as dst_amount_usd
    , amount_received_native::number as dst_amount_native
    , amount_received_adjusted as dst_amount_adjusted
    , destination_token_decimals as dst_decimals
    , destination_token_symbol as dst_symbol
    , depositor
    , recipient
    , dst_chain_id
    , destination_token_address as dst_token_address
    , destination_chain as dst_chain
    , null as token_address
    , null as token_chain
    , protocol_fee
    , null as bridge_message_app
    , null as version
    , 'debridge' as app
    , unique_id
FROM 
    {{ref('fact_debridge_transfers_with_price_and_metadata')}}
WHERE
    src_timestamp <= to_date(sysdate())
    and (source_chain is not null and destination_chain is not null)
union all
select
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , amount_sent as src_amount_usd
    , amount_sent_native::number as src_amount_native
    , amount_sent_adjusted as src_amount_adjusted
    , src_decimals
    , src_symbol
    , src_chain
    , origin_chain_id as src_chain_id
    , origin_token as src_token_address
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , amount_received as dst_amount_usd
    , amount_received_native::number as dst_amount_native
    , amount_received_adjusted as dst_amount_adjusted
    , dst_decimals
    , dst_symbol
    , depositor
    , recipient
    , destination_chain_id as dst_chain_id
    , destination_token as dst_token_address
    , dst_chain
    , token_address
    , token_chain
    , protocol_fee
    , bridge_message_app
    , version
    , app
    , unique_id
from {{ ref('fact_superchain_bridge_transfers') }}
where
    (src_block_timestamp <= to_date(sysdate()) or src_block_timestamp is null)
    and (dst_block_timestamp <= to_date(sysdate()) or dst_block_timestamp is null)
    and (src_chain is not null and dst_chain is not null)
