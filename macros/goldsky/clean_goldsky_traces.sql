{% macro clean_goldsky_traces(chain) %}
with
    raw_traces as (
        select 
            parquet_raw:block_number::number as block_number
            , parquet_raw:block_timestamp::timestamp as block_timestamp
            , parquet_raw:block_hash::string as block_hash
            , parquet_raw:transaction_hash::string as transaction_hash
            , parquet_raw:transaction_index::number as transaction_index
            , parquet_raw:from_address::string as from_address
            , parquet_raw:to_address::string as to_address
            , parquet_raw:value::string as value
            , parquet_raw:input::string as input
            , parquet_raw:output::string as output
            , parquet_raw:trace_type::string as trace_type
            , parquet_raw:call_type::string as call_type
            , parquet_raw:reward_type::string as reward_type
            , parquet_raw:gas::string as gas
            , parquet_raw:gas_used::string as gas_used
            , parquet_raw:subtraces::string as subtraces
            , parquet_raw:trace_address::string as trace_address
            , parquet_raw:error::string as error
            , parquet_raw:status::number as status
            , parquet_raw:trace_id::string as trace_id
            , parquet_raw:id::string as id
        from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_traces_parquet") }}
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
from raw_traces
qualify row_number() over (partition by trace_id order by block_timestamp desc) = 1
{% endmacro %}