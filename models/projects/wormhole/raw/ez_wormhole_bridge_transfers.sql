{{
    config(
        materialized="view",
        snowflake_warehouse="WORMHOLE",
        database="wormhole",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}

select
    t.id as wormhole_txn_hash
    , timestamp
    , date_trunc('day', timestamp) as date
    , c1.chain as source_chain
    , c2.chain as destination_chain
    , from_address as source_address
    , to_address as destiantion_address
    , symbol as token_symbol
    , token_address
    , amount
    , amount_usd
from {{ref('fact_wormhole_transfers')}} t
inner join {{ref('wormhole_chain_ids')}} c1 on t.from_chain = c1.id
inner join {{ref('wormhole_chain_ids')}} c2 on t.to_chain = c2.id