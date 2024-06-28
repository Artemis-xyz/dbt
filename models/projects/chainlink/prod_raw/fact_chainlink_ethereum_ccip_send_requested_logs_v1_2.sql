{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ccip_send_requested_logs_v1_2",
    )
}}

select
    'ethereum' as chain
    , contract_address
    , decoded_log
    , tx_hash
    , block_number
    , block_timestamp
    , event_index
    , origin_from_address as tx_from
    , decoded_log:"message":"feeTokenAmount"::number as fee_token_amount
    , decoded_log:"message":"sourceChainSelector"::string as origin_selector
    , decoded_log:"message":"feeToken"::string as fee_token
    , onramp_meta.chain_selector as destination_selector
    , onramp_meta.chain as destination_chain
FROM ethereum_flipside.core.ez_decoded_event_logs logs
left join {{ref('dim_chainlink_ethereum_ccip_onramp_meta')}} onramp_meta on lower(onramp_meta.onramp) = lower(contract_address)
where topics[0]::string = '0xd0c3c799bf9e2639de44391e7f524d229b2b55f5b1ea94b2bf7da42f7243dddd' -- CCIPSendRequested v1.2.0