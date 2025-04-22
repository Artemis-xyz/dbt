{{ config(
    materialized='table',
    unique_key='unique_id',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:tx_hash::string as tx_hash,
        PARQUET_RAW:block_time::timestamp as block_time,
        PARQUET_RAW:slot_no::integer as slot_no,
        PARQUET_RAW:txidx::integer as txidx,
        PARQUET_RAW:out_sum::numeric as out_sum,
        PARQUET_RAW:fee::numeric as fee,
        PARQUET_RAW:deposit::numeric as deposit,
        PARQUET_RAW:size::integer as size,
        PARQUET_RAW:invalid_before::numeric as invalid_before,
        PARQUET_RAW:invalid_after::numeric as invalid_after,
        PARQUET_RAW:valid_script::boolean as valid_script,
        PARQUET_RAW:script_size::integer as script_size,
        PARQUET_RAW:count_inputs::integer as count_inputs,
        PARQUET_RAW:count_outputs::integer as count_outputs,
        -- Generate a unique ID for each record
        md5(concat(tx_hash)) as unique_id
    from {{ source('PROD_LANDING', 'raw_cardano_transactions_parquet') }}
)

select 
    epoch_no,
    tx_hash,
    block_time,
    slot_no,
    txidx,
    out_sum,
    fee,
    deposit,
    size,
    invalid_before,
    invalid_after,
    valid_script,
    script_size,
    count_inputs,
    count_outputs,
    unique_id
from source_data
qualify row_number() over (partition by unique_id order by block_time desc) = 1 