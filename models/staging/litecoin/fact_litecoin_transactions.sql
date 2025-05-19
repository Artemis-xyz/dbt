{{ config(
    materialized='incremental',
    unique_key='unique_id',
    snowflake_warehouse='LITECOIN'
) }}

with raw_transactions as (
    select
        parquet_raw:hash::string as transaction_hash,
        parquet_raw:size::bigint as size,
        parquet_raw:virtual_size::bigint as virtual_size,
        parquet_raw:version::bigint as version,
        parquet_raw:lock_time::bigint as lock_time,
        parquet_raw:block_hash::string as block_hash,
        parquet_raw:block_number::bigint as block_number,
        parquet_raw:block_timestamp::timestamp as block_timestamp,
        parquet_raw:block_timestamp_month::date as block_timestamp_month,
        parquet_raw:input_count::bigint as input_count,
        parquet_raw:output_count::bigint as output_count,
        parquet_raw:input_value::bigint as input_value,
        parquet_raw:output_value::bigint as output_value,
        parquet_raw:is_coinbase::boolean as is_coinbase,
        parquet_raw:fee::bigint as fee,
        parquet_raw:inputs as inputs,
        parquet_raw:outputs as outputs,
        md5(parquet_raw:hash::string) as unique_id
    from {{ source('PROD_LANDING', 'raw_litecoin_transactions_parquet') }}
    where 1=1
    {% if is_incremental() %}
      and parquet_raw:block_number::bigint > (select max(block_number) from {{ this }})
    {% endif %}
)

select *
from raw_transactions
qualify row_number() over (partition by unique_id order by block_timestamp desc) = 1 