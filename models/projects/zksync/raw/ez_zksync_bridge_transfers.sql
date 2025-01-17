{{
    config(
        materialized="view",
        snowflake_warehouse="ZKSYNC",
        database="zksync",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select 
    block_timestamp,
    tx_hash as ethereum_tx_hash,
    event_index,
    depositor as source_address,
    recipient as destination_address,
    amount,
    token_address as ethereum_token_address,
    source_chain,
    destination_chain
from {{ref('fact_zksync_era_bridge_transfers')}}