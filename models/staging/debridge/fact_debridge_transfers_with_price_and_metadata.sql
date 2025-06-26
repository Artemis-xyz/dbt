{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

select
    transfers.order_id
    , transfers.src_timestamp
    , transfers.amount_sent_native
    , transfers.source_chain
    , transfers.source_token_decimals
    , transfers.source_token_symbol
    , transfers.source_token_address
    , transfers.amount_sent_adjusted
    , transfers.amount_sent
    , transfers.source_tx_hash
    , transfers.amount_received_native
    , transfers.amount_received_adjusted
    , transfers.amount_received
    , transfers.destination_chain
    , transfers.destination_token_decimals
    , transfers.destination_token_symbol
    , transfers.destination_token_address
    , transfers.fix_fee_native
    , transfers.fix_fee_adjusted
    , transfers.fix_fee
    , transfers.percentage_fee_native
    , transfers.percentage_fee_adjusted
    , transfers.percentage_fee
    , transfers.category
    , metadata.sender as depositor
    , metadata.receiver as recipient
    , metadata.dst_block_timestamp
    , coalesce(percentage_fee,0) + coalesce(fix_fee,0) as protocol_fee
    , source_chain_ids.id as src_chain_id 
    , destination_chain_ids.id as dst_chain_id
    , concat(coalesce(transfers.order_id, 'null'), '|', 'debridge') as unique_id
from {{ref('fact_debridge_transfers_with_prices')}} as transfers
left join {{ ref('fact_debridge_order_metadata') }} as metadata 
on transfers.order_id = metadata.order_id
left join {{ ref('dim_chain_ids') }} as source_chain_ids 
on transfers.source_chain = source_chain_ids.chain
left join {{ ref('dim_chain_ids') }} as destination_chain_ids 
on transfers.destination_chain = destination_chain_ids.chain
