{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

SELECT 
    distinct date_trunc('day', block_timestamp) as date,
    MAX(block_number) as last_block
FROM {{ source('ETHEREUM_FLIPSIDE', 'fact_blocks') }}
WHERE block_timestamp >= DATE('2022-12-12')
GROUP BY date_trunc('day', block_timestamp)