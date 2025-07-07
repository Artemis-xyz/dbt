{{
    config(
        materialized="view",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select 
    block_timestamp,
    transaction_hash as ethereum_tx_hash,
    event_index,
    depositor as source_address,
    recipient as destination_address,
    amount_native as amount,
    token_address as ethereum_token_address,
    source_chain,
    destination_chain
from {{ref('fact_optimism_bridge_transfers')}}