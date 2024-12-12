{{ config(materialized="incremental", unique_key="id") }}
with
    raw_traces as (
        select 
            parquet_raw:block_number as block_number
            , parquet_raw:block_timestamp::timestamp as block_timestamp
            , parquet_raw:block_hash as block_hash
            , parquet_raw:transaction_hash as transaction_hash
            , parquet_raw:transaction_index as transaction_index
            , parquet_raw:from_address as from_address
            , parquet_raw:to_address as to_address
            , parquet_raw:value as value
            , parquet_raw:input as input
            , parquet_raw:output as output
            , parquet_raw:trace_type as trace_type
            , parquet_raw:call_type as call_type
            , parquet_raw:reward_type as reward_type
            , parquet_raw:gas as gas
            , parquet_raw:gas_used as gas_used
            , parquet_raw:subtraces as subtraces
            , parquet_raw:trace_address as trace_address
            , parquet_raw:error as error
            , parquet_raw:status as status
            , parquet_raw:trace_id as trace_id
            , parquet_raw:id as id
        from {{ source("PROD_LANDING", "raw_celo_traces_parquet") }}
        {% if is_incremental() %}
            where block_timestamp::timestamp >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    )
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
from raw_receipt_transactions
qualify row_number() over (partition by id order by block_timestamp desc) = 1