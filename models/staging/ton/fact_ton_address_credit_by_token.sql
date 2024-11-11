{{
    config(
        materialized="table",
        unique_key=["block_timestamp", "address"],
        snowflake_warehouse="TON_MD",
    )
}}
select
    to_address as address,
    contract_address,
    block_timestamp,
    cast(amount as float) as credit,
    null as credit_usd,
    tx_hash,
    null as trace_index,
    event_index
from {{ ref("fact_ton_token_transfers") }}
where
    to_address is not null
    and to_date(block_timestamp) < to_date(sysdate())
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}