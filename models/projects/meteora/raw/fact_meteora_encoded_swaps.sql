
{{
    config(
        materialized='incremental',
        snowflake_warehouse='METEORA',
        database='METEORA',
        schema='raw',
        unique_key=['block_timestamp', '_log_id'],
    )
 }}

-- TODO: microbatch config to add back later
-- event_time='block_timestamp',
WITH log_id_add_query as (
    SELECT
        tx_id,
        instruction,
        instruction:data::STRING as encoded_data,
        instruction:accounts[0]::STRING as event_authority,
        ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (instruction_index * 1000 + inner_index)) as event_number,
        CONCAT(tx_id,'-',event_number,
                CAse
                    WHEN program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' THEN '-meteora dlmm pools program'
                    WHEN program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB' THEN '-meteora pools program'
                END) AS log_id
    FROM {{source('SOLANA_FLIPSIDE', 'fact_events_inner')}}
    WHERE (program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' OR program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB')
    AND ((program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo' AND substr(encoded_data, 0, 9) = 'yCGxBopjn' AND event_authority = 'D1ZN9Wj1fRSUQfCjhvnu1hqDMT7hzjzBBpi12nVniYD6')
        OR
        (program_id = 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'))
    {% if is_incremental() %}
        and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
), ez_dex_swaps_encoded_fees as (
    SELECT
        dex_swaps.*,
        inner_events.encoded_data
       -- function(inner_events.encoded_data) as decoded_data --apply decoder
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }} dex_swaps
    LEFT JOIN log_id_add_query inner_events
        ON dex_swaps._log_id = inner_events.log_id
    WHERE program_id = 'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo'
    and encoded_data is not null
    {% if is_incremental() %}
        and block_timestamp > (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% endif %}
)
select *
from ez_dex_swaps_encoded_fees