{{ config(snowflake_warehouse="BALANCES_LG", materialized="incremental", unique_key="unique_id")}}

select
    block_timestamp
    , block_number
    , transaction_hash
    , to_address as address
    , contract_address
    , event_index
    , -1 as trace_index
    , amount_raw as credit_raw
    , amount_native as credit_native
    , unique_id
from {{ref("fact_stellar_token_transfers")}}   
where lower(event_type) not in ('burn')
    and block_timestamp::date < to_date(sysdate())
{% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}