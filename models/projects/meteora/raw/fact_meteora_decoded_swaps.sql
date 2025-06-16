
{{
    config(
        materialized='incremental',
        snowflake_warehouse='SNOWPARK_WAREHOUSE',
        full_refresh=false,
    )
 }}

-- TODO: microbatch config to add back later
-- incremental_strategy='microbatch',
-- event_time='block_timestamp',
-- begin='2023-11-27', 
-- batch_size='day',
-- concurrent_batches=true,

with swaps as (
    SELECT
        *
    FROM {{ ref('fact_meteora_encoded_swaps') }}
    WHERE 1=1
    {% if is_incremental() %}
        and block_timestamp > (select MAX(block_timestamp) from {{ this }})
    {% endif %}
)
SELECT
    swaps.*,
    PARSE_JSON(DECODE_SOLANA_INNER_INSTRUCTION(idl::STRING, encoded_data)) as decoded_data
FROM SOLANA_FLIPSIDE.CORE.dim_idls idls, swaps
WHERE lower(idls.program_id) = lower('LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo')
