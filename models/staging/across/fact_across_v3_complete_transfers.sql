{{config(materialized='incremental', unique_key=['deposit_id', 'origin_chain_id', 'dst_tx_hash'], snowflake_warehouse="ACROSS_V2")}}

with filled_relay_events as (
    select
        messaging_contract_address as dst_messaging_contract_address,
        block_timestamp as dst_block_timestamp,
        tx_hash as dst_tx_hash,
        event_index as dst_event_index,
        deposit_id,
        src_amount,
        dst_amount,
        depositor,
        recipient,
        destination_chain_id,
        origin_token,
        destination_token,
        origin_chain_id,
        realized_lp_fee_pct as dst_realized_lp_fee_pct,
        relayer_fee_pct as dst_relayer_fee_pct,
        protocol_fee as dst_protocol_fee,
        message as dst_message,
        chain as dst_chain,
        decoded_log
    from {{ref('fact_across_v3_filledrelay')}}
    {% if is_incremental() %}
        where block_timestamp >= (select dateadd('day', -3, least(max(src_block_timestamp), max(dst_block_timestamp))) from {{ this }})
    {% endif %}
)
, funds_deposited_events as (
    select
        messaging_contract_address as src_messaging_contract_address,
        block_timestamp as src_block_timestamp,
        tx_hash src_tx_hash,
        event_index as src_event_index,
        deposit_id,
        src_amount,
        dst_amount,
        depositor,
        recipient,
        destination_chain_id,
        origin_token,
        destination_token,
        origin_chain_id,
        realized_lp_fee_pct as src_realized_lp_fee_pct,
        relayer_fee_pct as src_relayer_fee_pct,
        protocol_fee as src_protocol_fee,
        message as src_message,
        chain as src_chain,
        decoded_log
    from {{ref('fact_across_v3_fundsdeposited')}}
    {% if is_incremental() %}
        where block_timestamp >= (select dateadd('day', -3, least(max(src_block_timestamp), max(dst_block_timestamp))) from {{ this }})
    {% endif %}
)
select
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , coalesce(src.src_amount, dst.src_amount) as src_amount
    , src_relayer_fee_pct
    , src_chain
    , coalesce(src.origin_token, dst.origin_token) as origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , coalesce(dst.dst_amount, src.dst_amount) dst_amount
    , coalesce(src.depositor, dst.depositor) as depositor
    , coalesce(dst.recipient, src.recipient) as recipient
    , coalesce(src.destination_chain_id, dst.destination_chain_id) as destination_chain_id
    , coalesce(dst.destination_token, src.destination_token) as destination_token
    , coalesce(src.origin_chain_id, dst.origin_chain_id) as origin_chain_id
    , src_realized_lp_fee_pct as realized_lp_fee_pct
    , dst_relayer_fee_pct
    , dst_message
    , dst_chain
    , coalesce(src.deposit_id, dst.deposit_id) as deposit_id
    , coalesce(dst.dst_protocol_fee, src.src_protocol_fee) as protocol_fee
from funds_deposited_events as src
full join filled_relay_events as dst
on src.deposit_id = dst.deposit_id 
and src.origin_chain_id = dst.origin_chain_id
