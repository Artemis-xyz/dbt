{{ config(materialized="table") }}
with
    raw_receipt_transactions as (
        select
            parquet_raw:block_hash::string block_hash,
            parquet_raw:block_number block_number,
            parquet_raw:block_timestamp::int block_timestamp,
            parquet_raw:from_address::string from_address,
            parquet_raw:gas::decimal gas,
            parquet_raw:gas_price::decimal gas_price,
            parquet_raw:gas_used::decimal gas_used,
            case
                when try_cast(parquet_raw:l1_fee::string as decimal) is not null
                then try_cast(parquet_raw:l1_fee::string as decimal)
                else 0
            end as l1_fee,
            parquet_raw:receipt_status::int receipt_status,
            parquet_raw:id::string id,
            parquet_raw:input::string input,
            parquet_raw:transaction_type::int transaction_type,
            parquet_raw:to_address::string to_address,
            parquet_raw:hash::string hash,
            parquet_raw:value::string value
        from {{ source("PROD_LANDING", "raw_zora_receipt_transactions_parquet") }}
    )
select *
from raw_receipt_transactions
qualify row_number() over (partition by hash order by block_timestamp desc) = 1
