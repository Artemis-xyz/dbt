{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

with
decoded_debridge_data as (
    -- https://docs.debridge.finance/dln-the-debridge-liquidity-network-protocol/fees-and-supported-chains
    select 
        value:"orderId":"stringValue"::string as order_id
        , value:"giveOfferWithMetadata":"amount":"bigIntegerValue"::number as amount_sent_native
        , value:"giveOfferWithMetadata":"chainId":"bigIntegerValue"::number as source_chain_id
        , value:"giveOfferWithMetadata":"decimals"::number as source_token_decimals
        , value:"giveOfferWithMetadata":"symbol"::string as source_token_symbol
        , value:"giveOfferWithMetadata":"tokenAddress":"stringValue"::string as source_token_address
        , value:"createEventTransactionHash":"stringValue"::string as source_tx_hash
        , value:"takeOfferWithMetadata":"amount":"bigIntegerValue"::number as amount_received_native
        , value:"takeOfferWithMetadata":"chainId":"bigIntegerValue"::number as destination_chain
        , value:"takeOfferWithMetadata":"decimals"::number as destination_token_decimals
        , value:"takeOfferWithMetadata":"symbol"::string as destination_token_symbols
        , value:"takeOfferWithMetadata":"tokenAddress":"stringValue"::string as destination_token_address
        , value:"fixFee":"bigIntegerValue"::number as fix_fee_native -- this is in the chain native token 
        , value:"finalPercentFee":"bigIntegerValue"::number as percentage_fee_native -- this is in the input token amount
        , extraction_date
    from LANDING_DATABASE.PROD_LANDING.raw_debridge_orders t1,
    lateral flatten(input => parse_json(source_json)) as flat_json
)
select
    order_id,
    max_by(amount_sent_native, extraction_date) as src_timestamp,
    max_by(source_chain_id, extraction_date) as src_tx_hash,
    max_by(source_token_decimals, extraction_date) as source_token_decimals,
    max_by(source_token_symbol, extraction_date) as source_token_symbol,
    max_by(source_token_address, extraction_date) as source_token_address,
    max_by(source_tx_hash, extraction_date) as source_tx_hash,
    max_by(amount_received_native, extraction_date) as amount_received_native,
    max_by(destination_chain, extraction_date) as destination_chain,
    max_by(destination_token_decimals, extraction_date) as destination_token_decimals,
    max_by(destination_token_symbols, extraction_date) as destination_token_symbols,
    max_by(destination_token_address, extraction_date) as destination_token_address,
    max_by(fix_fee_native, extraction_date) as fix_fee_native,
    max_by(percentage_fee_native, extraction_date) as percentage_fee_native,
    extraction_date
from decoded_debridge_data t1
GROUP BY order_id, extraction_date
