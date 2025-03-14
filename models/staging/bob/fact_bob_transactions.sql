{{ config(materialized="table", snowflake_warehouse="BOB", unique_key=["transaction_hash"]) }}
with
    raw_receipt_transactions as (
        select
        parquet_raw:hash::string as transaction_hash
        , parquet_raw:block_hash::string as block_hash
        , parquet_raw:block_number::integer as block_number
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
            when try_cast(parquet_raw:receipt_gas_used::string as integer) is not null
            then try_cast(parquet_raw:receipt_gas_used::string as integer)
            else 0
        end as receipt_gas_used
        , parquet_raw:receipt_status::string as receipt_status
        , case
            when try_cast(parquet_raw:effective_gas_price::string as integer) is not null
            then try_cast(parquet_raw:effective_gas_price::string as integer)
            else 0
        end as effective_gas_price
        , case
            when try_cast(parquet_raw:l1_fee::string as integer) is not null
            then try_cast(parquet_raw:l1_fee::string as integer)
            else 0
        end as l1_fee
        from {{ source("PROD_LANDING", "raw_bob_transactions_parquet") }}
        {% if is_incremental() %}
            where parquet_raw:block_timestamp::timestamp_ntz >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select *
from raw_receipt_transactions
qualify row_number() over (partition by transaction_hash order by block_timestamp desc) = 1
