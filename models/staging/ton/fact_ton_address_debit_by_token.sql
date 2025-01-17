{{
    config(
        materialized="table",
        unique_key=["block_timestamp", "address"],
        snowflake_warehouse="TON_MD",
    )
}}

select
    from_address as address,
    contract_address,
    block_timestamp,
    cast(amount * -1 as float) as debit,
    null as debit_usd,
    tx_hash,
    null as trace_index,
    event_index
from {{ ref("fact_ton_token_transfers")}}
where
    to_date(block_timestamp) < to_date(sysdate())
    and from_address is not null
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}