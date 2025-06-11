{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

with
decoded_debridge_data as (
    -- https://docs.debridge.finance/dln-the-debridge-liquidity-network-protocol/fees-and-supported-chains
    select 
        value:"orderId":"stringValue"::string as order_id
        , value:"creationTimestamp"::timestamp as src_timestamp
        , value:"giveOfferWithMetadata":"amount":"bigIntegerValue"::string as amount_sent_native
        , value:"giveOfferWithMetadata":"chainId":"bigIntegerValue"::number as source_chain_id
        , value:"giveOfferWithMetadata":"decimals"::number as source_token_decimals
        , value:"giveOfferWithMetadata":"symbol"::string as source_token_symbol
        , value:"giveOfferWithMetadata":"tokenAddress":"stringValue"::string as source_token_address
        , value:"createEventTransactionHash":"stringValue"::string as source_tx_hash
        , value:"takeOfferWithMetadata":"amount":"bigIntegerValue"::string  amount_received_native
        , value:"takeOfferWithMetadata":"chainId":"bigIntegerValue"::number as destination_chain_id
        , value:"takeOfferWithMetadata":"decimals"::number as destination_token_decimals
        , value:"takeOfferWithMetadata":"symbol"::string as destination_token_symbol
        , value:"takeOfferWithMetadata":"tokenAddress":"stringValue"::string as destination_token_address
        , value:"fixFee":"bigIntegerValue"::number as fix_fee_native -- this is in the chain native token 
        , value:"finalPercentFee":"bigIntegerValue"::number as percentage_fee_native -- this is in the input token amount
        , value:"state"::string as state
        , extraction_date
    from {{ source('PROD_LANDING', 'raw_debridge_orders') }} as t1,
    lateral flatten(input => parse_json(source_json)) as flat_json
), 
latest_data as (
    select
        order_id,
        max_by(src_timestamp, extraction_date) as src_timestamp,
        max_by(amount_sent_native, extraction_date) as amount_sent_native,
        max_by(source_chain_id, extraction_date) as source_chain_id,
        max_by(source_token_decimals, extraction_date) as source_token_decimals,
        max_by(source_token_symbol, extraction_date) as source_token_symbol,
        max_by(source_token_address, extraction_date) as source_token_address,
        max_by(source_tx_hash, extraction_date) as source_tx_hash,
        max_by(amount_received_native, extraction_date) as amount_received_native,
        max_by(destination_chain_id, extraction_date) as destination_chain_id,
        max_by(destination_token_decimals, extraction_date) as destination_token_decimals,
        max_by(destination_token_symbol, extraction_date) as destination_token_symbol,
        max_by(destination_token_address, extraction_date) as destination_token_address,
        max_by(fix_fee_native, extraction_date) as fix_fee_native,
        max_by(percentage_fee_native, extraction_date) as percentage_fee_native,
        max_by(state, extraction_date) as state,
        max(extraction_date) as extraction_date
    from decoded_debridge_data t1
    GROUP BY order_id
)
select 
    order_id,
    src_timestamp,
    IFF(length(amount_sent_native) < 38, amount_sent_native::number, SUBSTRING(amount_sent_native, 1, 39)::number) as amount_sent_native,
    IFF(length(amount_sent_native) < 38, null, SUBSTRING(amount_sent_native, 39)::number) as amount_sent_native_remainder,
    src_chain.chain as source_chain,
    source_token_decimals,
    source_token_symbol,
    source_token_address,
    source_tx_hash,
    IFF(length(amount_received_native) < 38, amount_received_native::number, SUBSTRING(amount_received_native, 1, 39)::number) as amount_received_native,
    IFF(length(amount_received_native) < 38, null, SUBSTRING(amount_received_native, 39)::number) as amount_received_native_remainder,
    dst_chain.chain as destination_chain,
    destination_token_decimals,
    destination_token_symbol,
    destination_token_address,
    fix_fee_native,
    src_chain.coingecko_id as fee_chain_coingecko_id,
    src_chain.decimals as fee_token_decimals,
    percentage_fee_native,
    state
from latest_data
left join {{ ref('fact_debridge_chain_id') }} as src_chain
    on latest_data.source_chain_id = src_chain.id
left join {{ ref('fact_debridge_chain_id') }} as dst_chain
    on latest_data.destination_chain_id = dst_chain.id
where state in ('SentUnlock', 'Fulfilled', 'ClaimedUnlock')
