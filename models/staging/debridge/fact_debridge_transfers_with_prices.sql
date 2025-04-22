{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

WITH all_prices as (
    {{ get_coingecko_prices_on_chains(['solana', 'berachain', 'ethereum', 'abstract', 'bsc', 'polygon', 'arbitrum', 'base', 'avalanche', 'sonic', 'fantom', 'story', 'hyperliquid', 'gnosis', 'optimism']) }}
),
coalesced_prices as (
    select
        contract_address
        , date
        , max(price) as price
        , max(decimals) as decimals
        , max(symbol) as symbol
    from all_prices
    group by contract_address, date
),
eth_price as (
    select
        '0x0000000000000000000000000000000000000000' as contract_address
        , date
        , price 
        , decimals
        , symbol
    from coalesced_prices
    where lower(contract_address) = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
prices as (
    select
        contract_address
        , date
        , price
        , decimals
        , symbol
    from coalesced_prices
    union all
    select 
        contract_address
        , date
        , price
        , decimals
        , symbol 
    from eth_price
),
token_fee_prices as (
    {{ get_multiple_coingecko_price_with_latest_by_ids(['ethereum', 'avalanche-2', 'binancecoin', 'matic-network', 'fantom', 'solana', 'dai', 'sonic-3', 'berachain-bera', 'story-2', 'hyperliquid', 'crossfi-2', 'neon', 'metis-token', 'bitrock', 'cronos-zkevm-cro']) }}
)
select
    order_id,
    src_timestamp,
    amount_sent_native,
    source_chain,
    source_token_decimals,
    source_token_symbol,
    source_token_address,
    amount_sent_native / POW(10, source_token_decimals) as amount_sent_adjusted,
    (amount_sent_native / POW(10, source_token_decimals)) * source_prices.price as amount_sent,
    source_tx_hash,
    amount_received_native,
    amount_received_native / POW(10, destination_token_decimals) as amount_received_adjusted,
    (amount_received_native / POW(10, destination_token_decimals)) * destination_prices.price as amount_received,
    destination_chain,
    destination_token_decimals,
    destination_token_symbol,
    destination_token_address,
    fix_fee_native,
    fix_fee_native / POW(10, fee_token_decimals) as fix_fee_adjusted,
    (fix_fee_native / POW(10, fee_token_decimals)) * fee_prices.price as fix_fee,
    percentage_fee_native,
    percentage_fee_native / POW(10, source_token_decimals) as percentage_fee_adjusted,
    (percentage_fee_native / POW(10, source_token_decimals)) * source_prices.price as percentage_fee,
    case when contains(coalesce(lower(t.source_token_symbol), lower(t.destination_token_symbol)), 'usd') then 'Stablecoin' else 'Token' end as category
from {{ref('fact_debridge_transfers')}} as t
left join prices as source_prices on lower(t.source_token_address) = lower(source_prices.contract_address) and date_trunc('day', t.src_timestamp) = source_prices.date
left join prices as destination_prices on lower(t.source_token_address) = lower(destination_prices.contract_address) and date_trunc('day', t.src_timestamp) = destination_prices.date
left join token_fee_prices as fee_prices on lower(t.fee_chain_coingecko_id) = lower(fee_prices.coingecko_id) and date_trunc('day', t.src_timestamp) = fee_prices.date
