{{
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

-- 521 rows as expected
select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:accountedInterest_::number as accountedInterest_
    , decoded_log:issuanceRate_::float as issuanceRate_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'AccountingStateUpdated'