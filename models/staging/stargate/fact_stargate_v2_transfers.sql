{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
select
    OFTReceived.block_timestamp as dst_block_timestamp
    , OFTReceived.tx_hash as dst_tx_hash
    , OFTReceived.event_index as dst_event_index
    , dst_chain
    , dst_address

    , OFTSent.block_timestamp as src_block_timestamp
    , OFTSent.tx_hash as src_tx_hash
    , OFTSent.event_index as src_event_index
    , src_chain
    , src_address

    , OFTReceived.token_address as dst_token_address
    , OFTReceived.decimals as dst_decimals
    , OFTReceived.symbol as dst_symbol

    , OFTSent.token_address as src_token_address
    , OFTSent.decimals as src_decimals
    , OFTSent.symbol as src_symbol

    , OFTReceived.amount_received_native
    , OFTReceived.amount_received_adjusted
    , OFTReceived.amount_received

    , OFTSent.amount_sent_native
    , OFTSent.amount_sent_adjusted
    , OFTSent.amount_sent
    
    , amount_sent_native - amount_received_native as fee_amount_native
    , amount_sent_adjusted - amount_received_adjusted as fee_amount_adjusted
    , amount_sent - amount_received as fee_amount

    , guid

    , fees.fees_native
    , fees.fees_usd

    , fees_usd + fee_amount as fees
    

from {{ref("fact_stargate_v2_event_OFTSent")}} as OFTSent
inner join {{ref("fact_stargate_v2_event_OFTReceived")}} as OFTReceived 
    using(guid, src_chain, dst_chain)
left join {{ref("fact_stargate_fees")}} as fees
    on OFTSent.tx_hash = fees.tx_hash