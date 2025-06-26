
{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "event_index", "trace_index"],
        snowflake_warehouse="BALANCES_MD",
    )
}}

select
    max_by(to_address, block_timestamp) as address,
    max_by(contract_address, block_timestamp) as contract_address,
    max(block_timestamp) as block_timestamp,
    max_by(cast(raw_amount as float), block_timestamp) as credit,
    tx_hash,
    -1 as trace_index,
    event_index
from sei_flipside.core_evm.ez_token_transfers
where
    to_address <> lower('0x0000000000000000000000000000000000000000')
    and to_date(block_timestamp) < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
group by tx_hash, trace_index, event_index
    
union all

select
    to_address as address,
    'eip155:1329:native' as contract_address,
    block_timestamp,
    amount as credit,
    tx_hash,
    trace_index,
    -1 as event_index
from sei_flipside.core_evm.ez_native_transfers
where
    to_date(block_timestamp) < to_date(sysdate())
    and to_address <> lower('0x0000000000000000000000000000000000000000')
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}


