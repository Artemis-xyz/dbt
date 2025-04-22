{{ config(
    materialized='table',
    unique_key='unique_id',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:slot_no::integer as slot_no,
        PARQUET_RAW:txidx::integer as txidx,
        PARQUET_RAW:tx_hash::string as tx_hash,
        -- Generate a unique ID for each record
        md5(concat(epoch_no::string, slot_no::string, txidx::string)) as unique_id
    from {{ source('PROD_LANDING', 'raw_cardano_tx_hash_parquet') }}
)

select 
    epoch_no,
    slot_no,
    txidx,
    tx_hash,
    unique_id
from source_data
qualify row_number() over (partition by unique_id order by epoch_no desc, slot_no desc) = 1 