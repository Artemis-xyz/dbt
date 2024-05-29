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
    t2.tx_hash,
    coalesce(t1.to_address, t2.from_address) as contract_address,
    t2.block_timestamp,
    date_trunc('day', t2.block_timestamp) raw_date,
    coalesce(t2.from_address, t2.to_address) as from_address,
    t2.actual_fee / 1E18 as tx_fee,
    t2.actual_fee_unit as currency,
    (tx_fee * price) gas_usd,
    'starknet' as chain,
    new_contracts.name,
    new_contracts.app,
    new_contracts.friendly_name,
    new_contracts.sub_category,
    new_contracts.category --EOA Transfers are more complicated on Starknet
from starknet_data_warehouse__t1.starknet.calls t1
left join starknet_data_warehouse__t1.starknet.transactions t2 on t1.tx_hash = t2.tx_hash
left join new_contracts on lower(t1.to_address) = lower(new_contracts.address)
left join prices on raw_date = prices.date and curr = t2.actual_fee_unit
where
    t1.invocation_type = 'execute' and t1.call_depth = 0
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and t2.block_timestamp
        >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
    {% endif %}