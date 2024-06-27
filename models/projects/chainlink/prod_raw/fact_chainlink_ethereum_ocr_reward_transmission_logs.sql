{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ocr_reward_transmission_logs",
    )
}}

select 
    'ethereum' as chain
    , block_timestamp
    , block_number
    , tx_hash
    , contract_address
    , event_index
    , event_name
    , decoded_log
    , tx_status
from ethereum_flipside.core.ez_decoded_event_logs 
where topics[0]::string = '0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6'
{% if is_incremental() %}
    and block_timestamp >= (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}