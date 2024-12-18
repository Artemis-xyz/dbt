{{
    config(
        materialized="view",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select
    s.chain as origin_chain
    , origin_tx_hash
    , origin_block_timestamp
    , depositor as origin_address
    , origin_token_address
    , origin_token_symbol
    , origin_token_amount
    , d.chain as destination_chain
    , destination_tx_hash
    , destination_block_timestamp
    , recipient as destination_address
    , destination_token_address
    , destination_token_symbol
    , destination_token_amount
from {{ref('fact_synapse_transfers')}}
left join {{ref('chain_ids')}} d on destination_chain_id = d.id
left join {{ref('chain_ids')}} s on origin_chain_id = s.id
