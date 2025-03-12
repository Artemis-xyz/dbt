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
    , coalesce(src_timestamp, dst_timestamp) as timestamp
    , date_trunc('day', coalesce(src_timestamp, dst_timestamp)) as date
    , source_chain
    , destination_chain
    , from_address as source_address
    , to_address as destiantion_address
    , symbol as token_symbol
    , token_address
    , amount
    , amount_usd
from {{ref('fact_wormhole_operations_with_price')}} t
