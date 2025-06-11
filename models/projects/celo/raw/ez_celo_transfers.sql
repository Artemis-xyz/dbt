{{
    config(
        materialized="incremental",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_transfers",
        unique_key=["transaction_hash", "event_index", "trace_index"]
    )
}}

select
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , trace_index
    , -1 as event_index
    , contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_native
    , amount
    , price
from {{ref('fact_celo_native_token_transfers')}}
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
    and contract_address = 'eip155:42220:native'
{% endif %}
union all
select
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , '-1' as trace_index
    , event_index
    , contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_native
    , amount
    , price
from {{ref('fact_celo_token_transfers')}}
{% if is_incremental() %}
    where block_timestamp >= (select max(block_timestamp) from {{ this }})
    and contract_address <> 'eip155:42220:native'
{% endif %}