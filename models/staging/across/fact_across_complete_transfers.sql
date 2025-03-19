{{config(materialized='table', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS")}}
WITH
prices as (
    {{ get_coingecko_prices_on_chains(['ethereum', 'optimism', 'arbitrum', 'polygon', 'base', 'ink', 'soneium', 'linea', 'worldchain', 'unichain', 'zksync']) }}
),
across_transfers as (
    select
        src_messaging_contract_address
        , src_block_timestamp
        , src_tx_hash
        , src_event_index
        , src_amount
        , src_chain
        , origin_chain_id
        , origin_token
        , dst_messaging_contract_address
        , dst_block_timestamp
        , dst_tx_hash
        , dst_event_index
        , dst_amount
        , depositor
        , recipient
        , destination_chain_id
        , destination_token
        , dst_message
        , dst_chain
        , deposit_id
        , protocol_fee
        , 'across' as bridge_message_app
        , '3' as version
    from {{ ref('fact_across_v3_complete_transfers') }}
    union all
    select
        src_messaging_contract_address
        , src_block_timestamp
        , src_tx_hash
        , src_event_index
        , src_amount
        , src_chain
        , origin_chain_id
        , origin_token
        , dst_messaging_contract_address
        , dst_block_timestamp
        , dst_tx_hash
        , dst_event_index
        , dst_amount
        , depositor
        , recipient
        , destination_chain_id
        , destination_token
        , dst_message
        , dst_chain
        , deposit_id
        , protocol_fee
        , 'across' as bridge_message_app
        , '2' as version
    from {{ ref('fact_across_v2_complete_transfers') }}
)

SELECT 
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , src_prices.symbol as src_symbol
    , src_prices.decimals as src_decimals
    , src_prices.price as src_token_price
    , src_amount/POW(10, src_prices.decimals) * src_prices.price as amount_sent
    , src_amount as amount_sent_native
    , src_amount/POW(10, src_prices.decimals) as amount_sent_adjusted
    , src_chain
    , origin_chain_id
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , dst_amount
    , dst_prices.symbol as dst_symbol
    , dst_prices.decimals as dst_decimals
    , dst_prices.price as dst_token_price
    , dst_amount/POW(10, dst_prices.decimals) * dst_prices.price as amount_received
    , dst_amount as amount_received_native
    , dst_amount/POW(10, dst_prices.decimals) as amount_received_adjusted
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , dst_chain
    , protocol_fee
    , bridge_message_app
    , version
FROM
    across_transfers
LEFT JOIN prices as src_prices
    ON lower(origin_token) = lower(src_prices.contract_address) and date_trunc('day', src_block_timestamp) = src_prices.date
LEFT JOIN prices as dst_prices
    ON lower(destination_token) = lower(dst_prices.contract_address) and date_trunc('day', dst_block_timestamp) = dst_prices.date
