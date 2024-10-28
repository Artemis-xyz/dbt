{{
    config(
        materialized='incremental', 
        snowflake_warehouse='MAPLE',
    )
}}

-- All rows present
with fixed_term_loan_pools as (
    select
        distinct decoded_log:instance_::string as instance_address
    from
        ethereum_flipside.core.ez_decoded_event_logs
    where
        event_name= 'InstanceDeployed'
        and lower(contract_address) = lower('0x36a7350309B2Eb30F3B908aB0154851B5ED81db0')
)

select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:principalPaid_::number as principalPaid_
    , decoded_log:interestPaid_::number as interestPaid_
    , decoded_log:fees_::string as fees_
from
    ethereum_flipside.core.ez_decoded_event_logs
where
    event_name = 'PaymentMade'
    and contract_address in (select instance_address from fixed_term_loan_pools)
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}

union all

select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(data, 3, 64)) as principalPaid_
    , PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(data, 64+3, 64)) as interestPaid_
    , PC_DBT_DB.PROD.HEX_TO_INT(SUBSTR(data, 128+3, 64)) as fees_
from ethereum_flipside.core.fact_event_logs
where topics[0] = lower('0xcf358e925a8e033c6db877f18d10df6f21cd04ef165537bad5fc814eb23af960')
and contract_address in (select instance_address from fixed_term_loan_pools)
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}