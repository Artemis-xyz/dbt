{{
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

-- 96 rows as expected
select
    block_timestamp
    , tx_hash
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