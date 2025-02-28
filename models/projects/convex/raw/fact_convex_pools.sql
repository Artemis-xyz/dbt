{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'CONVEX',
        database = 'CONVEX',
        schema = 'raw',
        alias = 'fact_convex_pools'
    )
}}

  SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    decoded_input_data:_lptoken::string as lptoken,
    decoded_input_data:_gauge::string as gauge,
    decoded_input_data:_stashVersion::number as stash_version,
    ROW_NUMBER() OVER (ORDER BY block_number) - 1 as pid
  FROM {{ source('ETHEREUM_FLIPSIDE', 'fact_decoded_traces') }}
  WHERE to_address = lower('0xF403C135812408BFbE8713b5A23a04b3D48AAE31')
    AND function_name = 'addPool'
    AND trace_status = 'SUCCESS'