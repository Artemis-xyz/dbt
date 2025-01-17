{{
    config(
        materialized="view",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_traces",
    )
}}

select
    block_number
    , block_timestamp
    , block_hash
    , transaction_hash
    , transaction_index
    , from_address
    , to_address
    , value
    , input
    , output
    , trace_type
    , call_type
    , reward_type
    , gas
    , gas_used
    , subtraces
    , trace_address
    , error
    , status
    , trace_id
    , id
from {{ref("fact_celo_traces")}}