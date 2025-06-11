{{
    config(
        materialized="incremental",
        unique_key="tx_hash",
        snowflake_warehouse="STARKNET_SM",
    )
}}

with
    new_contracts as (
        select distinct
            address,
            contract.name,
            contract.chain,
            contract.category,
            contract.sub_category,
            contract.app,
            contract.friendly_name
        from {{ ref("dim_contracts_gold") }} as contract
        where chain = 'starknet'
    ),
    prices as (
        select date, price, 'WEI' as curr
        from (
            {{ get_coingecko_price_with_latest("ethereum") }}
        )
        union all 
        select date, price, 'FRI' as curr
        from (
            {{ get_coingecko_price_with_latest("starknet") }}
        )
    )
select 
    t1.block_time as block_timestamp,
    t1.transaction_hash_hex as tx_hash,
    t2.contract_address_hex as contract_address,
    t1.block_date as raw_date,
    coalesce(t1.sender_address_hex, t1.contract_address_hex) as from_address,
    t1.actual_fee_amount / 1E18 as tx_fee,
    t1.actual_fee_unit as currency,
    (tx_fee * price) gas_usd,
    'starknet' as chain,
    t1.execution_status as status,

    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    new_contracts.category --EOA Transfers are more complicated on Starknet

from zksync_dune.starknet.transactions t1
left join zksync_dune.starknet.calls t2 
    on t1.transaction_hash_hex = t2.transaction_hash_hex
    and t1.sender_address_hex = t2.caller_address_hex
    and callstack_index = [0]
left join new_contracts on lower(t2.contract_address_hex) = lower(new_contracts.address)
left join prices on raw_date = prices.date and curr = t1.actual_fee_unit
where coalesce(t1.sender_address_hex, t1.contract_address_hex) is not null
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and t1.block_time
        >= (select DATEADD('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}