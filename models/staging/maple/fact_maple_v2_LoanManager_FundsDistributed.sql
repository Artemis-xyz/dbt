{{
    config(
        materialized="incremental",
        unique_key= ['tx_hash', 'event_index'],
        snowflake_warehouse="MAPLE",
    )
}}

select
    block_timestamp
    , tx_hash
    , event_index
    , block_number as block
    , contract_address
    , decoded_log:loan_::string as loan_
    , decoded_log:principal_::number as principal_
    , decoded_log:netInterest_::number as netInterest_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'FundsDistributed'
    and decoded_log:loan_ is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}