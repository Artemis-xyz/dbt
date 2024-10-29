{{
    config(
        materialized='incremental',
        unique_key= ['tx_hash', 'event_index'],
        snowflake_warehouse='MAPLE',
    )
}}

-- exact number of rows
SELECT
    block_timestamp
    , tx_hash
    , event_index
    , block_number as block
    , contract_address
    , decoded_log:assets::number as assets_
    , decoded_log:"owner"::string as owner_
    , decoded_log:requestId::string as requestId_
    , decoded_log:"shares"::number as shares_
FROM
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'RequestProcessed'
    and decoded_log:requestId is not null
    and decoded_log:"owner" is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}