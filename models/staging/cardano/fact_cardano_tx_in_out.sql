{{ config(
    materialized='incremental',
    unique_key='unique_id',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:slot_no::integer as slot_no,
        PARQUET_RAW:txidx::integer as txidx,
        PARQUET_RAW:inputs::variant as inputs,
        PARQUET_RAW:outputs::variant as outputs,
        -- Generate a unique ID for each record
        md5(concat(epoch_no::string, slot_no::string, txidx::string)) as unique_id
    from {{ source('PROD_LANDING', 'raw_cardano_tx_in_out_parquet') }}
    where 1=1
    {% if is_incremental() %}
      and (PARQUET_RAW:epoch_no::integer, PARQUET_RAW:slot_no::integer) > (select coalesce(max(epoch_no), 0), coalesce(max(slot_no), 0) from {{ this }})
    {% endif %}
)

select 
    epoch_no,
    slot_no,
    txidx,
    inputs,
    outputs,
    unique_id
from source_data
qualify row_number() over (partition by unique_id order by epoch_no desc, slot_no desc) = 1 