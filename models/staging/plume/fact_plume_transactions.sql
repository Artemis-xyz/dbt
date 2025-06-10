{{ config(materialized="table", snowflake_warehouse="PLUME", unique_key=["transaction_hash"]) }}
with
    raw_receipt_transactions as (
        select
        parquet_raw:transaction_hash::string as transaction_hash
        , parquet_raw:nonce::string as nonce
        , parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_number::integer as block_number
        , parquet_raw:transaction_index::integer as transaction_index
        , parquet_raw:from_address::string as from_address
        , parquet_raw:to_address::string as to_address
        , parquet_raw:value::integer as value
        , parquet_raw:gas::integer as gas
        , parquet_raw:gas_price::integer as gas_price
        , parquet_raw:input::string as input
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
        , parquet_raw:transaction_type::string as transaction_type
        , parquet_raw:block_timestamp::timestamp_ntz as block_timestamp
        , case
            when try_cast(parquet_raw:receipt_cumulative_gas_used::string as integer) is not null
            then try_cast(parquet_raw:receipt_cumulative_gas_used::string as integer)
            else 0
        end as receipt_cumulative_gas_used
        , case
            when try_cast(parquet_raw:receipt_gas_used::string as integer) is not null
            then try_cast(parquet_raw:receipt_gas_used::string as integer)
            else 0
        end as receipt_gas_used
        , parquet_raw:receipt_contract_address::string as receipt_contract_address
        , parquet_raw:receipt_status::string as receipt_status
        , case
            when try_cast(parquet_raw:receipt_effective_gas_price::string as integer) is not null
            then try_cast(parquet_raw:receipt_effective_gas_price::string as integer)
            else 0
        end as receipt_effective_gas_price
        , parquet_raw:receipt_root_hash::string as receipt_root_hash
        , case
            when try_cast(parquet_raw:receipt_l1_fee::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_fee::string as integer)
            else 0
        end as receipt_l1_fee
        , case
            when try_cast(parquet_raw:receipt_l1_gas_used::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_gas_used::string as integer)
            else 0
        end as receipt_l1_gas_used
        , case
            when try_cast(parquet_raw:receipt_l1_gas_price::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_gas_price::string as integer)
            else 0
        end as receipt_l1_gas_price
        , case
            when try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_fee_scalar::string as integer)
            else 0
        end as receipt_l1_fee_scalar
        , case
            when try_cast(parquet_raw:receipt_l1_blob_base_fee::string as integer) is not null
            then try_cast(parquet_raw:receipt_l1_blob_base_fee::string as integer)
            else 0
        end as receipt_l1_blob_base_fee
        , parquet_raw:receipt_l1_blob_base_fee_scalar::integer as receipt_l1_blob_base_fee_scalar
        , parquet_raw:blob_versioned_hashes::string as blob_versioned_hashes
        , case
            when try_cast(parquet_raw:max_fee_per_blob_gas::string as integer) is not null
            then try_cast(parquet_raw:max_fee_per_blob_gas::string as integer)
            else 0
        end as max_fee_per_blob_gas
        , parquet_raw:receipt_l1_block_number::integer as receipt_l1_block_number
        , parquet_raw:receipt_l1_base_fee_scalar::integer as receipt_l1_base_fee_scalar
        , parquet_raw:gateway_fee::integer as gateway_fee
        , parquet_raw:fee_currency::string as fee_currency
        , parquet_raw:gateway_fee_recipient::string as gateway_fee_recipient
        from {{ source("PROD_LANDING", "raw_plume_mainnet_transactions_parquet") }}
        {% if is_incremental() %}
            where parquet_raw:block_timestamp::timestamp_ntz >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select *
from raw_receipt_transactions
qualify row_number() over (partition by transaction_hash order by block_timestamp desc) = 1
