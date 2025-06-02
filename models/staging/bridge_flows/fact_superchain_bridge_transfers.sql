{{ config(materialized="table", snowflake_warehouse='BRIDGE_MD') }}

with
    flows_by_super_chain as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_base_bridge_transfers"),
                    ref("fact_optimism_bridge_transfers"),
                    ref("fact_soneium_bridge_transfers"),
                    ref("fact_unichain_bridge_transfers"),
                    ref("fact_worldchain_bridge_transfers"),
                ]
            )
        }}
    )
select
    src_messaging_contract_address
    , src_block_timestamp
    , src_transaction_hash as src_tx_hash
    , src_event_index
    , amount_native as src_amount
    , amount_native as amount_sent_native
    , amount_adjusted as amount_sent_adjusted
    , amount_usd as amount_sent
    , decimals as src_decimals
    , source_token_symbol as src_symbol
    , source_chain as src_chain
    , src_ids.id as origin_chain_id
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_transaction_hash as dst_tx_hash
    , dst_event_index
    , amount_native as dst_amount
    , amount_native as amount_received_native
    , amount_adjusted as amount_received_adjusted
    , amount_usd as amount_received
    , decimals as dst_decimals
    , destination_token_symbol as dst_symbol
    , depositor
    , recipient
    , dst_ids.id as destination_chain_id
    , destination_token
    , destination_chain as dst_chain
    , null as token_address
    , null as token_chain
    , null as protocol_fee
    , null as bridge_message_app
    , null as version
    , 'superchain_bridge' as app
from flows_by_super_chain
left join {{ ref('dim_chain_ids') }} as src_ids on flows_by_super_chain.source_chain = src_ids.chain
left join {{ ref('dim_chain_ids') }} as dst_ids on flows_by_super_chain.destination_chain = dst_ids.chain
