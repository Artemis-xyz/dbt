{{ config(snowflake_warehouse="SEI", materialized="incremental", unique_key=["transaction_hash", "event_index"]) }}

with evm_transfers as (
    select
        et.block_timestamp,
        et.block_number,
        et.event_index,
        et.transaction_hash,
        et.contract_address,
        et.from_address,
        et.to_address,
        et.amount_raw,
        et.amount_native,
        et.amount,
        et.price
    from {{ ref('fact_sei_evm_token_transfers') }} et
    where lower(et.contract_address) in (
        select lower(contract_address)
        from {{ ref('fact_sei_token_contracts') }}
    )
    {% if is_incremental() %}
        and et.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
),

cosmos_transfers as (
    select
        ct.block_timestamp,
        ct.block_number,
        ct.event_index,
        ct.transaction_hash,
        ct.contract_address,
        ct.from_address,
        ct.to_address,
        ct.amount_raw,
        ct.amount_native,
        ct.amount,
        ct.price
    from {{ ref('fact_sei_cosmos_token_transfers') }} ct
    where lower(ct.contract_address) in (
        select lower(contract_address)
        from {{ ref('fact_sei_token_contracts') }}
    )
    {% if is_incremental() %}
        and ct.block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select
    block_timestamp,
    block_number,
    event_index,
    transaction_hash,
    contract_address,
    from_address,
    to_address,
    amount_raw,
    amount_native,
    amount,
    price
from evm_transfers
union all
select
    block_timestamp,
    block_number,
    event_index,
    transaction_hash,
    contract_address,
    from_address,
    to_address,
    amount_raw,
    amount_native,
    amount,
    price
from cosmos_transfers