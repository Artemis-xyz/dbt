{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="BRIDGE_MD",
    )
}}
-- TODO: Break out from initiated on l1 and finalized on l2 vs initiated on l2 and finalized on l1
with 
    l1_transfers as 
    (
        select
            *
        from {{ ref('fact_optimism_l1_bridge_transfers') }}
        {% if is_incremental() %}
            where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    ),
    l2_transfers as 
    (
        select
            *
        from {{ ref('fact_optimism_l2_bridge_transfers') }}
        {% if is_incremental() %}
            where block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
    select 
        l1.block_timestamp as src_block_timestamp,
        l1.tx_hash as src_tx_hash,
        l1.event_index as src_event_index,
        l1.depositor as src_depositor,
        l1.recipient as src_recipient,
        l1.amount as src_amount,
        l1.token_address as src_token_address,
        l1.source_chain as src_source_chain,
        l1.destination_chain as src_destination_chain,
        l2.block_timestamp as dst_block_timestamp,
        l2.tx_hash as dst_tx_hash,
        l2.event_index as dst_event_index,
        l2.depositor as dst_depositor,
        l2.recipient as dst_recipient,
        l2.amount as dst_amount,
        l2.token_address as dst_token_address,
        0 as fee
    from l1_transfers l1 
    full join l2_transfers l2
