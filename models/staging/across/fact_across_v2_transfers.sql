{{
    config(
        materialized="table",
        unique_key=["tx_hash", "event_index"],
        snowflake_warehouse="ACROSS"
    )
}}

SELECT
    src_messaging_contract_address as contract_address
    , src_block_timestamp as block_timestamp
    , src_tx_hash as tx_hash
    , src_event_index as event_index
    , coalesce(dst_amount, src_amount) as amount
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , origin_chain_id
    , realized_lp_fee_pct
    , dst_relayer_fee_pct as relayer_fee_pct
    , null as destination_token_symbol
from {{ref('fact_across_v2_complete_transfers')}}
