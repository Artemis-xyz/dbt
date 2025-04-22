{{ config(
    materialized='table',
    unique_key='block_hash',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:slot_no::integer as slot_no,
        PARQUET_RAW:block_time::timestamp as block_time,
        PARQUET_RAW:block_size::integer as block_size,
        PARQUET_RAW:tx_count::integer as tx_count,
        PARQUET_RAW:sum_tx_fee::integer as sum_tx_fee,
        PARQUET_RAW:script_count::integer as script_count,
        PARQUET_RAW:sum_script_size::integer as sum_script_size,
        PARQUET_RAW:pool_hash::string as pool_hash,
        PARQUET_RAW:block_hash::string as block_hash
    from {{ source('PROD_LANDING', 'raw_cardano_blocks_parquet') }}
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
    block_hash
from source_data 