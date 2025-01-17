{{
    config(
        materialized="view",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}



select 
    version
    , contract_address
    , block_timestamp
    , event_index
    , t3.chain as source_chain
    , depositor as source_address
    , tx_hash as destination_tx_hash
    
    , t2.chain as destination_chain
    , recipient as destination_address
    , destination_token_symbol
    , destination_token
    , amount
from {{ref('fact_across_transfers')}} t1
left join {{ref('chain_ids')}} t2 on t1.destination_chain_id = t2.id
left join {{ref('chain_ids')}} t3 on t1.origin_chain_id = t3.id
