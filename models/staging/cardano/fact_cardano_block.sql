{{ config(
    materialized='table',
    unique_key='unique_id',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:slot_no::integer as slot_no,
        to_timestamp(PARQUET_RAW:block_time::number / 1000000) as block_time,
        PARQUET_RAW:block_size::integer as block_size,
        PARQUET_RAW:tx_count::integer as tx_count,
        PARQUET_RAW:sum_tx_fee::integer as sum_tx_fee,
        PARQUET_RAW:script_count::integer as script_count,
        PARQUET_RAW:sum_script_size::integer as sum_script_size,
        PARQUET_RAW:pool_hash::string as pool_hash,
        PARQUET_RAW:block_hash::string as block_hash,
        -- Generate a unique ID for each record
        md5(concat(block_hash)) as unique_id
    from {{ source('PROD_LANDING', 'raw_cardano_block_parquet') }}
)

select 
    epoch_no,
    slot_no,
    block_time,
    block_size,
    tx_count,
    sum_tx_fee,
    script_count,
    sum_script_size,
    pool_hash,
    block_hash,
    unique_id
from source_data
qualify row_number() over (partition by unique_id order by block_time desc) = 1 