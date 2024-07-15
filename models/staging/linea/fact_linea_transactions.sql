{{ config(materialized="table") }}
with
    raw_transactions_with_columns as (
        select
            parquet_raw:block_hash::string block_hash,
            parquet_raw:block_number block_number,
            parquet_raw:block_timestamp::int block_timestamp,
            parquet_raw:from_address::string from_address,
            parquet_raw:gas::decimal gas,
            parquet_raw:gas_price::decimal gas_price,
            parquet_raw:id::string id,
            parquet_raw:input::string input,
            parquet_raw:to_address::string to_address,
            parquet_raw:hash::string hash,
            parquet_raw:value::string value
        from {{ source("PROD_LANDING", "raw_linea_transactions_parquet") }}
    ),
    raw_traces_with_columns as (
        select
            parquet_raw:id::string id,
            parquet_raw:transaction_hash::string transaction_hash
        from {{ source("PROD_LANDING", "raw_linea_traces_parquet") }}
    ),
    raw_receipts_with_columns as (
        select
            parquet_raw:id::string id,
            parquet_raw:transaction_hash::string transaction_hash,
            parquet_raw:gas_used::decimal gas_used,
            parquet_raw:effective_gas_price::decimal effective_gas_price
        from {{ source("PROD_LANDING", "raw_linea_receipts_parquet") }}
    ),
    linea_transactions as (
        select
            raw_transactions_with_columns.*,
            raw_receipts_with_columns.gas_used,
            raw_receipts_with_columns.effective_gas_price,
            raw_traces_with_columns.transaction_hash is not null as receipt_status
        from raw_transactions_with_columns
        left join
            raw_receipts_with_columns
            on raw_transactions_with_columns.hash
            = raw_receipts_with_columns.transaction_hash
        left join
            raw_traces_with_columns
            on raw_transactions_with_columns.hash
            = raw_traces_with_columns.transaction_hash
        qualify row_number() over (partition by hash order by block_timestamp desc) = 1
    )
select *
from linea_transactions
