{{ config(
    materialized='incremental',
    unique_key='unique_id',
    snowflake_warehouse='CARDANO'
) }}

with source_data as (
    select 
        PARQUET_RAW:epoch_no::integer as epoch_no,
        PARQUET_RAW:stake_addr_hash::string as stake_addr_hash,
        PARQUET_RAW:pool_hash::string as pool_hash,
        PARQUET_RAW:amount::numeric as amount,
        -- Generate a unique ID for each record
        md5(concat(epoch_no::string, stake_addr_hash, pool_hash)) as unique_id
    from {{ source('PROD_LANDING', 'raw_cardano_epoch_stake_parquet') }}
    where 1=1
    {% if is_incremental() %}
      and PARQUET_RAW:epoch_no::integer > (select coalesce(max(epoch_no), 0) from {{ this }})
    {% endif %}
)

select 
    epoch_no,
    stake_addr_hash,
    pool_hash,
    amount,
    unique_id
from source_data
qualify row_number() over (partition by unique_id order by epoch_no desc) = 1 