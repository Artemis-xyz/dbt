{{ 
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="CELO_MD"
    )
}}
with
prices as ({{get_multiple_coingecko_price_with_latest('celo')}})
, celo_token_transfers as ({{ token_transfer_events("celo") }})
select
    block_timestamp,
    block_number,
    transaction_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    celo_token_transfers.contract_address,
    from_address,
    to_address,
    amount,
    amount / pow(10, decimals) as amount_adjusted,
    amount_adjusted * price as amount_usd,
    tx_status
from celo_token_transfers
left join prices
    on block_timestamp::date = prices.date
    and lower(celo_token_transfers.contract_address) = lower(prices.contract_address)

