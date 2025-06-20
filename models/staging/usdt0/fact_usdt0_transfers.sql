{{ config(materialized="table", snowflake_warehouse='USDT0') }}

{{config(materialized="table", snowflake_warehouse='USDT0')}}
select
    OFTReceived.block_timestamp as dst_block_timestamp
    , OFTReceived.transaction_hash as dst_transaction_hash
    , OFTReceived.event_index as dst_event_index
    , dst_chain
    , dst_address
    , OFTSent.contract_address as src_messaging_contract_address
    , OFTReceived.contract_address as dst_messaging_contract_address
    , OFTSent.block_timestamp as src_block_timestamp
    , OFTSent.transaction_hash as src_transaction_hash
    , OFTSent.event_index as src_event_index
    , src_chain
    , src_address

    , OFTReceived.amount_received_raw
    , OFTReceived.amount_received_native
    , OFTReceived.amount_received

    , OFTSent.amount_sent_raw
    , OFTSent.amount_sent_native
    , OFTSent.amount_sent
    
    , guid
    
from {{ref("fact_usdt0_OFTSent")}} as OFTSent
inner join {{ref("fact_usdt0_OFTReceived")}} as OFTReceived 
    using(guid)
