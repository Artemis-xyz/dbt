{{ config(materialized="incremental", snowflake_warehouse="HYPERLIQUID", unique_key=["transaction_hash"]) }}

with raw_receipt_transactions as (
    select
        parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_number::string as block_number
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , parquet_raw:transaction_hash::string as transaction_hash
        , parquet_raw:from_address::string as from_address
        , parquet_raw:to_address::string as to_address
        , parquet_raw:gas::integer as gas
        , parquet_raw:gas_price::integer as gas_price
        , parquet_raw:id::string as id
        , parquet_raw:nonce::integer as nonce
        , parquet_raw:receipt_cumulative_gas_used::integer as receipt_cumulative_gas_used
        , parquet_raw:receipt_effective_gas_price::integer as receipt_effective_gas_price
        , parquet_raw:receipt_gas_used::float as receipt_gas_used 
        , parquet_raw:input::string as input
        , case
            when try_cast(parquet_raw:max_fee_per_blob_gas::string as integer) is not null
            then try_cast(parquet_raw:max_fee_per_blob_gas::string as integer)
            else 0
        end as max_fee_per_blob_gas
        , case
            when try_cast(parquet_raw:max_fee_per_gas::string as integer) is not null
            then try_cast(parquet_raw:max_fee_per_gas::string as integer)
            else 0
        end as max_fee_per_gas
        , case
            when try_cast(parquet_raw:max_priority_fee_per_gas::string as integer) is not null
            then try_cast(parquet_raw:max_priority_fee_per_gas::string as integer)
            else 0
        end as max_priority_fee_per_gas
        , case
            when try_cast(parquet_raw:receipt_l1_blob_base_fee::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_blob_base_fee::string as integer)
            else 0
        end as receipt_l1_blob_base_fee
        , case
            when try_cast(parquet_raw:receipt_l1_fee::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_fee::string as integer)
            else 0
        end as receipt_l1_fee
        , case
            when try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer)
            else 0
        end as receipt_l1_fee_scalar
        , case
            when try_cast(parquet_raw:receipt_l1_gas_price::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_gas_price::string as integer)
            else 0
        end as receipt_l1_gas_price
        , case
            when try_cast(parquet_raw:receipt_l1_gas_used::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_gas_used::string as integer)
            else 0
        end as receipt_l1_gas_used
        , parquet_raw:receipt_status::number as receipt_status
        , parquet_raw:transaction_index::number as transaction_index
        , parquet_raw:transaction_type::number as transaction_type
        , parquet_raw:value::string as value
        from {{ source("PROD_LANDING", "raw_hyperevm_transactions_parquet") }}
        {% if is_incremental() %}
            where parquet_raw:block_timestamp::timestamp_ntz >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select
    block_hash
    , block_number
    , block_timestamp
    , transaction_hash
    , from_address
    , to_address
    , gas
    , gas_price
    , id
    , nonce
    , receipt_cumulative_gas_used
    , receipt_effective_gas_price
    , receipt_gas_used
    , input
    , max_fee_per_blob_gas
    , max_fee_per_gas
    , max_priority_fee_per_gas
    , receipt_l1_blob_base_fee
    , receipt_l1_fee
    , receipt_l1_fee_scalar
    , receipt_l1_gas_price
    , receipt_l1_gas_used
    , receipt_status
    , transaction_index
    , transaction_type
    , value
from raw_receipt_transactions
qualify row_number() over (partition by transaction_hash order by block_timestamp desc) = 1
