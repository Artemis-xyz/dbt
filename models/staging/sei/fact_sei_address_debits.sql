{{
    config(
        materialized="incremental",
        unique_key="unique_id",
        snowflake_warehouse="SEI",
    )
}}

select
    block_timestamp
    , block_number
    , transaction_hash
    , from_address as address
    , contract_address
    , -1 as trace_index
    , -1 * amount_raw as debit_raw
    , -1 * amount_native as debit_native
    , unique_id
from {{ref("fact_sei_token_transfers")}}   
where lower(from_address) not in (lower('0x0000000000000000000000000000000000000000'))
    and block_timestamp::date < to_date(sysdate())
{% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}