
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='block_timestamp',
        begin='2023-11-27', 
        batch_size='day',
        concurrent_batches=true,
        snowflake_warehouse='SNOWPARK_WAREHOUSE',
        full_refresh=false,
    )
 }}
with swaps as (
    SELECT
        *
    FROM {{ ref('fact_meteora_encoded_swaps') }}
)
SELECT
    swaps.*,
    PARSE_JSON(DECODE_SOLANA_INNER_INSTRUCTION(idl::STRING, encoded_data)) as decoded_data
FROM SOLANA_FLIPSIDE.CORE.dim_idls idls, swaps
WHERE lower(idls.program_id) = lower('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo')
{% if is_incremental() %}
    and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
{% endif %}
