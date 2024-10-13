{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
    )
}}

-- outputs 9 rows less than expected
select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:interestPaid::number as interestPaid
    , decoded_log:latePayment::string as latePayment
    , decoded_log:nextPaymentDue::number as nextPaymentDue
    , decoded_log:paymentsRemaining::number as paymentsRemaining
    , decoded_log:principalOwed::number as principalOwed
    , decoded_log:principalPaid::number as principalPaid
    , decoded_log:totalPaid::number as totalPaid
from
    {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
where
    event_name = 'PaymentMade'
    and decoded_log:interestPaid is not null