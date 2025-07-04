{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key="unique_id") }}

with 
    prices as ({{get_multiple_coingecko_price_with_latest(chain)}})
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
            coalesce(t1.evm_tx_hash, ft.tx_id) as transaction_hash,
            lower(sc.evm_contract_address) as contract_address,
            case 
                when lower(transfer_type) = 'ibc_transfer_in' then '0x0000000000000000000000000000000000000000'
                else coalesce(from_map.evm_address, ft.sender)
            end as from_address,
            case 
                when lower(transfer_type) = 'ibc_transfer_out' then '0x0000000000000000000000000000000000000000'
                else coalesce(to_map.evm_address, ft.receiver)
            end as to_address,
            ft.amount as amount_raw,
            ft.fact_transfers_id as unique_id
        from sei_flipside.core.fact_transfers ft
        inner join {{ ref("fact_sei_cosmos_token_contracts") }} sc
            on lower(ft.currency) = lower(sc.wasm_contract_address)
        left join sei_flipside.core.dim_address_mapping from_map
            on lower(ft.sender) = lower(from_map.sei_address)
        left join sei_flipside.core.dim_address_mapping to_map
            on lower(ft.receiver) = lower(to_map.sei_address)
        left join {{ ref("fact_test_tx_hash_mapping") }} t1
            on lower(ft.tx_id) = lower(t1.wasm_tx_hash)
)
select
    block_timestamp
    , block_number
    , transaction_hash
    , token_transfers.contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_raw / pow(10, contract_addresses.decimals) as amount_native
    , amount_native * prices.price as amount
    , prices.price
    , unique_id
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
left join contract_addresses
    on lower(token_transfers.contract_address) = lower(contract_addresses.contract_address)
where amount_raw > 0