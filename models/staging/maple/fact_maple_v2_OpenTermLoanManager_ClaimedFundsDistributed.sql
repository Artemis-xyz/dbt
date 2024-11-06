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
    , decoded_log:delegateManagementFee_::number as delegateManagementFee_
    , decoded_log:delegateServiceFee_::number as delegateServiceFee_
    , decoded_log:platformManagementFee_::number as platformManagementFee_
    , decoded_log:platformServiceFee_::number as platformServiceFee_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'ClaimedFundsDistributed'
    and decoded_log:loan_ is not null
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}