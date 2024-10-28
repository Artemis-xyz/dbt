{{
    config(
        materialized='incremental',
        snowflake_warehouse='MAPLE',
    )
}}

select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:amountFunded::number as amountFunded
    , decoded_log:debtLocker::string as debtLocker
    , decoded_log:loan::string as loan
from
    {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
where
    event_name = 'LoanFunded'
    and decoded_log:debtLocker is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}