{{ config(
    materialized='incremental',
    unique_key='number',
    snowflake_warehouse='LITECOIN'
) }}
with source_data as (
    select 
        PARQUET_RAW:hash::string as hash,
        PARQUET_RAW:size::number as size,
        PARQUET_RAW:stripped_size::number as stripped_size,
        PARQUET_RAW:weight::number as weight,
        PARQUET_RAW:number::number as number,
        PARQUET_RAW:version::number as version,
        PARQUET_RAW:merkle_root::string as merkle_root,
        PARQUET_RAW:timestamp::timestamp as timestamp,
        PARQUET_RAW:timestamp_month::string as timestamp_month,
        PARQUET_RAW:nonce::string as nonce,
        PARQUET_RAW:bits::string as bits,
        PARQUET_RAW:coinbase_param::string as coinbase_param,
        PARQUET_RAW:transaction_count::number as transaction_count
    from {{ source('PROD_LANDING', 'raw_litecoin_blocks_parquet') }}
    where 1=1
    {% if is_incremental() %}
      and PARQUET_RAW:number::number > (select max(number) from {{ this }})
    {% endif %}
)

select 
    hash,
    size,
    stripped_size,
    weight,
    number,
    version,
    merkle_root,
    timestamp,
    timestamp_month,
    nonce,
    bits,
    coinbase_param,
    transaction_count
from source_data
qualify row_number() over (partition by number order by timestamp desc) = 1
