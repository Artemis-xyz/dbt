{{config(materialized="table", snowflake_warehouse='BAM_TRENDING_WAREHOUSE_LG')}}
select
    OFTReceived.block_timestamp as dst_block_timestamp
    , OFTReceived.tx_hash as dst_tx_hash
    , OFTReceived.event_index as dst_event_index
    , dst_chain
    , dst_address
    , OFTSent.stargate_implementation_pool as src_messaging_contract_address
    , OFTReceived.stargate_implementation_pool as dst_messaging_contract_address
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
    
    , case when amount_sent_native - amount_received_native < 0 then 0 else amount_sent_native - amount_received_native end as fees_native
    , case when amount_sent_adjusted - amount_received_adjusted < 0 then 0 else amount_sent_adjusted - amount_received_adjusted end as fees_adjusted
    , case when amount_sent - amount_received < 0 then 0 else amount_sent - amount_received end as fees

    -- token rewards
    , case when amount_sent_native - amount_received_native < 0 then abs(amount_sent_native - amount_received_native) else 0 end as token_rewards_native
    , case when amount_sent_adjusted - amount_received_adjusted < 0 then abs(amount_sent_adjusted - amount_received_adjusted) else 0 end as token_rewards_adjusted
    , case when amount_sent - amount_received < 0 then abs(amount_sent - amount_received) else 0 end as token_rewards

    , guid
    

from {{ref("fact_stargate_v2_event_OFTSent")}} as OFTSent
inner join {{ref("fact_stargate_v2_event_OFTReceived")}} as OFTReceived 
    using(guid, src_chain, dst_chain)
