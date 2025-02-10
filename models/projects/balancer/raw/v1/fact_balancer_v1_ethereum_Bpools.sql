{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
    )
}}
    
SELECT 
    block_timestamp,
    event_name,
    event_index,
    decoded_log:caller::STRING AS caller,
    decoded_log:pool::STRING AS pool 
FROM {{ source("ETHEREUM_FLIPSIDE", "ez_decoded_event_logs")}}
WHERE contract_address = lower('0x9424B1412450D0f8Fc2255FAf6046b98213B76Bd') -- Balancer v1 Core Pool Factory address 
AND event_name = 'LOG_NEW_POOL'