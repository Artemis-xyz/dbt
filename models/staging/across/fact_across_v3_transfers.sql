{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="ACROSS"
    )
}}

select 
    src_messaging_contract_address as contract_address
    , src_block_timestamp as block_timestamp
    , src_tx_hash as tx_hash
    , src_event_index as event_index
    , dst_amount as amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , src_amount as input_amount
    , origin_token as input_token
    , null as destination_token_symbol
from {{ref('fact_across_v3_complete_transfers')}}
