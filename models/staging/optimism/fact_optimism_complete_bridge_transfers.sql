{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}
-- TODO data quality check
with 
    withdrawals as 
    (
        select
            l2_withdrawal.block_timestamp as src_block_timestamp
            , l2_withdrawal.contract_address as src_contract_address
            , l2_withdrawal.tx_hash as src_tx_hash
            , l2_withdrawal.event_index as src_event_index
            , l2_withdrawal.amount_native as src_amount_native
            , l1_withdrawal.block_timestamp as dst_block_timestamp
            , l1_withdrawal.contract_address as dst_contract_address
            , l1_withdrawal.tx_hash as dst_tx_hash
            , l1_withdrawal.event_index as dst_event_index
            , l1_withdrawal.amount_native as dst_amount_native
            , l2_withdrawal.depositor as depositor
            , l2_withdrawal.recipient  as recipient
            , null as fee
            , l2_withdrawal.src_token_address
            , l2_withdrawal.dst_token_address
            , l2_withdrawal.source_chain
            , l2_withdrawal.destination_chain
        from {{ref("fact_optimism_l2_bridge_transfers")}} as l2_withdrawal
        full join {{ref("fact_optimism_l1_bridge_transfers")}} as l1_withdrawal
        on l2_withdrawal.depositor = l1_withdrawal.depositor 
        and l2_withdrawal.recipient = l1_withdrawal.recipient
        and l2_withdrawal.amount_native = l1_withdrawal.amount_native 
        and l2_withdrawal.dst_token_address = l1_withdrawal.dst_token_address 
        and l2_withdrawal.src_token_address = l2_withdrawal.src_token_address
        and l2_withdrawal.block_timestamp < l1_withdrawal.block_timestamp 
        where l2_withdrawal.action = 'withdrawal' and l1_withdrawal.action = 'withdrawal' 
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    ),
    deposits as 
    (
        select
            l1_withdrawal.block_timestamp as src_block_timestamp
            , l1_withdrawal.contract_address as src_contract_address
            , l1_withdrawal.tx_hash as src_tx_hash
            , l1_withdrawal.event_index as src_event_index
            , l1_withdrawal.amount_native as src_amount_native
            , l2_withdrawal.block_timestamp as dst_block_timestamp
            , l2_withdrawal.contract_address as dst_contract_address
            , l2_withdrawal.tx_hash as dst_tx_hash
            , l2_withdrawal.event_index as dst_event_index
            , l2_withdrawal.amount_native as dst_amount_native
            , l1_withdrawal.depositor as depositor
            , l1_withdrawal.recipient  as recipient
            , null as fee
            , l1_withdrawal.src_token_address
            , l1_withdrawal.dst_token_address
            , l1_withdrawal.source_chain
            , l1_withdrawal.destination_chain
        from {{ ref("fact_optimism_l1_bridge_transfers") }} as l1_withdrawal 
        full join {{ ref("fact_optimism_l2_bridge_transfers") }} as l2_withdrawal
        on l2_withdrawal.depositor = l1_withdrawal.depositor 
        and l2_withdrawal.recipient = l1_withdrawal.recipient
        and l2_withdrawal.amount_native = l1_withdrawal.amount_native 
        and l2_withdrawal.dst_token_address = l1_withdrawal.dst_token_address 
        and l2_withdrawal.src_token_address = l2_withdrawal.src_token_address
        and l2_withdrawal.block_timestamp < l1_withdrawal.block_timestamp 
        where l2_withdrawal.action = 'deposit' and l1_withdrawal.action = 'deposit' 
    )
    select 
        *
    from withdrawals
    union all
    select 
        *
    from deposits
