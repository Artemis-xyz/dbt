{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

with 
    prices as ({{get_multiple_coingecko_price_with_latest('sei')}})
    , contract_addresses as (
        select 
            distinct 
            contract_address,
            symbol,
            decimals
        from prices
    ), token_transfers as (
        select
            ft.block_timestamp,
            ft.block_id as block_number,
            ft.tx_id as transaction_hash,
            lower(ft.currency) as contract_address,
            case 
                when lower(transfer_type) = 'ibc_transfer_in' then '0x0000000000000000000000000000000000000000'
                else ft.sender
            end as from_address,
            case 
                when lower(transfer_type) = 'ibc_transfer_out' then '0x0000000000000000000000000000000000000000'
                else ft.receiver
            end as to_address,
            ft.amount as amount_raw,
            ft.fact_transfers_id as unique_id
        from sei_flipside.core.fact_transfers ft
        where tx_succeeded = true
        {% if is_incremental() %}
            and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select
    block_timestamp
    , block_number
    , row_number() over (partition by transaction_hash order by unique_id) as event_index
    , transaction_hash
    , token_transfers.contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_raw / pow(10, contract_addresses.decimals) as amount_native
    , amount_native * prices.price as amount
    , prices.price
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
left join contract_addresses
    on lower(token_transfers.contract_address) = lower(contract_addresses.contract_address)
where amount_raw > 0