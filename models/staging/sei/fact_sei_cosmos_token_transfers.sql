{{ config(snowflake_warehouse="SEI", materialized="table") }}

select
    ft.block_timestamp,
    ft.block_id as block_number,
    ft.tx_id as transaction_hash,
    sc.evm_contract_address as contract_address,
    coalesce(from_map.evm_address, ft.sender) as from_address,
    coalesce(to_map.evm_address, ft.receiver) as to_address,
    ft.amount as amount_raw
from sei_flipside.core.fact_transfers ft
inner join {{ ref("fact_sei_cosmos_token_contracts") }} sc
    on lower(ft.currency) = lower(sc.wasm_contract_address)
left join sei_flipside.core.dim_address_mapping from_map
    on lower(ft.sender) = lower(from_map.sei_address)
left join sei_flipside.core.dim_address_mapping to_map
    on lower(ft.receiver) = lower(to_map.sei_address)