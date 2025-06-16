{{config(materialized='table', unique_key=['tx_hash', 'chain', 'event_index'], snowflake_warehouse="ACROSS")}}
WITH
chain_prices as (
    {{ get_coingecko_prices_on_chains(['ethereum', 'optimism', 'arbitrum', 'polygon', 'base', 'ink', 'soneium', 'linea', 'worldchain', 'unichain', 'zksync']) }}
),
prices as (
    select
        date,
        contract_address,
        max(decimals) as decimals,
        max(symbol) as symbol,
        max(price) as price
    from chain_prices
    group by contract_address, date
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
),

transfer_with_price as  (
    SELECT 
        src_messaging_contract_address
        , src_block_timestamp
        , src_tx_hash
        , src_event_index
        , src_prices.symbol as src_symbol
        , src_prices.decimals as src_decimals
        , src_prices.price as src_token_price
        , (src_amount/POW(10, src_prices.decimals)) * src_prices.price as amount_sent
        , src_amount as amount_sent_native
        , (src_amount/POW(10, src_prices.decimals)) as amount_sent_adjusted
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
        , (dst_amount/POW(10, dst_prices.decimals)) * dst_prices.price as amount_received
        , dst_amount as amount_received_native
        , (dst_amount/POW(10, dst_prices.decimals)) as amount_received_adjusted
        , depositor
        , recipient
        , destination_chain_id
        , destination_token
        , dst_message
        , dst_chain
        , deposit_id
        , (protocol_fee/POW(10, coalesce(src_prices.decimals, dst_prices.decimals)) * coalesce(src_prices.price, dst_prices.price)) as protocol_fee
        , bridge_message_app
        , version
    FROM
        across_transfers
    LEFT JOIN prices as src_prices
        ON lower(origin_token) = lower(src_prices.contract_address) and date_trunc('day', src_block_timestamp) = src_prices.date
    LEFT JOIN prices as dst_prices
        ON lower(destination_token) = lower(dst_prices.contract_address) and date_trunc('day', dst_block_timestamp) = dst_prices.date
),
collapsed_data as (
    SELECT 
        max(src_messaging_contract_address) as src_messaging_contract_address
        , max(src_block_timestamp) as src_block_timestamp
        , max(src_tx_hash) as src_tx_hash
        , max(src_event_index) as src_event_index
        , max(src_symbol) as src_symbol
        , max(src_decimals) as src_decimals
        , max(src_token_price) as src_token_price
        , max(amount_sent) as amount_sent
        , max(amount_sent_native) as amount_sent_native
        , max(amount_sent_adjusted) as amount_sent_adjusted
        , max(src_chain) as src_chain
        , origin_chain_id
        , max(origin_token) as origin_token
        , max(dst_messaging_contract_address) as dst_messaging_contract_address
        , max(dst_block_timestamp) as dst_block_timestamp
        , max(dst_tx_hash) as dst_tx_hash
        , max(dst_event_index) as dst_event_index
        , max(dst_amount) as dst_amount
        , max(dst_symbol) as dst_symbol
        , max(dst_decimals) as dst_decimals
        , max(dst_token_price) as dst_token_price
        , max(amount_received) as amount_received
        , max(amount_received_native) as amount_received_native
        , max(amount_received_adjusted) as amount_received_adjusted
        , max(depositor) as depositor
        , max(recipient) as recipient
        , max(destination_chain_id) as destination_chain_id
        , max(destination_token) as destination_token
        , max(dst_message) as dst_message
        , max(dst_chain) as dst_chain
        , deposit_id
        , max(protocol_fee) as protocol_fee
        , max(bridge_message_app) as bridge_message_app
        , max(version) as version
    FROM
        transfer_with_price
    GROUP BY deposit_id, origin_chain_id
)
SELECT
    src_messaging_contract_address
    , src_block_timestamp
    , src_tx_hash
    , src_event_index
    , src_symbol
    , src_decimals
    , src_token_price
    , amount_sent
    , amount_sent_native
    , amount_sent_adjusted
    , coalesce(src_chains.chain, src_chain) as src_chain
    , origin_chain_id
    , origin_token
    , dst_messaging_contract_address
    , dst_block_timestamp
    , dst_tx_hash
    , dst_event_index
    , dst_amount
    , dst_symbol
    , dst_decimals
    , dst_token_price
    , amount_received
    , amount_received_native
    , amount_received_adjusted
    , depositor
    , recipient
    , destination_chain_id
    , destination_token
    , dst_message
    , coalesce(dst_chains.chain, dst_chain) as dst_chain
    , deposit_id
    , protocol_fee
    , bridge_message_app
    , version
    , concat(coalesce(TO_VARCHAR(deposit_id),'null'), '|', coalesce(TO_VARCHAR(origin_chain_id), 'null'), '|', 'across') as unique_id
from collapsed_data
left join {{ ref('dim_chain_ids') }} as src_chains on collapsed_data.origin_chain_id = src_chains.id
left join {{ ref('dim_chain_ids') }} as dst_chains on collapsed_data.destination_chain_id = dst_chains.id
