{% macro clean_goldsky_transactions(chain) %}
with 
data as (
    select
        parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_number::integer as block_number
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , parquet_raw:fee_currency::string as fee_currency
        , parquet_raw:from_address::string as from_address
        , coalesce(try_cast(parquet_raw:gas::string as integer), 0) as gas
        , coalesce(try_cast(parquet_raw:gas_price::string as integer), 0) as gas_price
        , parquet_raw:id::string as id
        , parquet_raw:input::string as input
        , coalesce(try_cast(parquet_raw:max_fee_per_gas::string as integer), 0) as max_fee_per_gas
        , coalesce(try_cast(parquet_raw:max_priority_fee_per_gas::string as integer), 0) as max_priority_fee_per_gas
        , parquet_raw:nonce::string as nonce
        , coalesce(try_cast(parquet_raw:receipt_cumulative_gas_used::string as integer), 0) as receipt_cumulative_gas_used
        , coalesce(try_cast(parquet_raw:receipt_effective_gas_price::string as integer), 0) as receipt_effective_gas_price
        , coalesce(try_cast(parquet_raw:receipt_gas_used::string as integer), 0) as receipt_gas_used
        , parquet_raw:receipt_status::string as receipt_status
        , parquet_raw:to_address::string as to_address
        , parquet_raw:transaction_hash::string as transaction_hash
        , parquet_raw:transaction_index::string as transaction_index
        , parquet_raw:transaction_type::string as transaction_type
        , parquet_raw:value::string as value
    from {{ source("PROD_LANDING", "raw_" ~ chain  ~ "_transactions_parquet") }}
    {% if is_incremental() %}
        where
            parquet_raw:block_timestamp::timestamp_ntz
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select
    block_hash
    , block_number
    , block_timestamp
    , transaction_hash
    , transaction_index
    , from_address
    , to_address
    , fee_currency
    , gas
    , gas_price
    , input
    , max_fee_per_gas
    , max_priority_fee_per_gas
    , nonce
    , receipt_cumulative_gas_used
    , receipt_effective_gas_price
    , receipt_gas_used
    , receipt_status
    , transaction_type
    , value
    , id
from data
qualify row_number() over (partition by transaction_hash order by block_timestamp desc) = 1
{% endmacro %}