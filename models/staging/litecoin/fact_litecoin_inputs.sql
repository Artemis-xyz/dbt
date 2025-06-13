{{ config(
    materialized='incremental',
    unique_key='unique_id',
    snowflake_warehouse='LITECOIN'
) }}
with source_data as (
    select 
        PARQUET_RAW:transaction_hash::string as transaction_hash,
        PARQUET_RAW:block_hash::string as block_hash,
        PARQUET_RAW:block_number::number as block_number,
        PARQUET_RAW:block_timestamp::timestamp as block_timestamp,
        PARQUET_RAW:index::number as index,
        PARQUET_RAW:spent_transaction_hash::string as spent_transaction_hash,
        PARQUET_RAW:spent_output_index::number as spent_output_index,
        PARQUET_RAW:script_asm::string as script_asm,
        PARQUET_RAW:script_hex::string as script_hex,
        PARQUET_RAW:sequence::number as sequence,
        PARQUET_RAW:required_signatures::number as required_signatures,
        PARQUET_RAW:type::string as type,
        PARQUET_RAW:addresses::array as addresses,
        PARQUET_RAW:value::number as value
    from {{ source('PROD_LANDING', 'raw_litecoin_inputs_parquet') }}
    where 1=1
    {% if is_incremental() %}
      and PARQUET_RAW:block_number::number > (select max(block_number) from {{ this }})
    {% endif %}
)

select 
    transaction_hash,
    block_hash,
    block_number,
    block_timestamp,
    index,
    spent_transaction_hash,
    spent_output_index,
    script_asm,
    script_hex,
    sequence,
    required_signatures,
    type,
    addresses,
    value,
    md5(transaction_hash || '-' || index) as unique_id
from source_data
