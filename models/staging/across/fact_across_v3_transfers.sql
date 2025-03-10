{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="ACROSS"
    )
}}

select 
    src_messaging_contract_address as contract_address
    , coalesce(src_block_timestamp, dst_block_timestamp) as block_timestamp
    , coalesce(src_tx_hash, dst_tx_hash) as tx_hash
    , coalesce(src_event_index, dst_event_index) as event_index
    , coalesce(dst_amount, src_amount) as amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , src_amount as input_amount
    , origin_token as input_token
    , null as destination_token_symbol
from {{ref('fact_across_v3_complete_transfers')}}
where coalesce(src_block_timestamp, dst_block_timestamp) is not null
