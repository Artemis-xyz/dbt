{{ config(
    materialized='table',
    unique_key='unique_id',
    snowflake_warehouse='LITECOIN'
) }}

with raw_transfers as (
    select
        parquet_raw:txid::string as tx_hash,
        parquet_raw:block_height::bigint as block_number,
        parquet_raw:block_time::timestamp as block_timestamp,
        parquet_raw:inputs as inputs,
        parquet_raw:outputs as outputs,
        -- Generate a unique ID for each transaction
        md5(tx_hash) as unique_id
    from {{ source('PROD_LANDING', 'raw_litecoin_transfers_parquet') }}
),

-- Flatten inputs
inputs_flattened as (
    select
        tx_hash,
        block_number,
        block_timestamp,
        value:address::string as from_address,
        value:value::bigint as input_amount,
        'input' as transfer_type
    from raw_transfers,
    lateral flatten(input => inputs)
),

-- Flatten outputs
outputs_flattened as (
    select
        tx_hash,
        block_number,
        block_timestamp,
        value:address::string as to_address,
        value:value::bigint as output_amount,
        'output' as transfer_type
    from raw_transfers,
    lateral flatten(input => outputs)
)

-- Combine inputs and outputs
select
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    input_amount,
    output_amount,
    transfer_type,
    unique_id
from (
    select * from inputs_flattened
    union all
    select * from outputs_flattened
)
qualify row_number() over (partition by unique_id, transfer_type order by block_timestamp desc) = 1 