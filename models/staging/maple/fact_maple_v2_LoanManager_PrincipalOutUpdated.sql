{{
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

-- 224 rows as expected
with loan_manager_addresses as (
    SELECT
        decoded_log:loanManager_::string as lm_address
    FROM
        {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
    where
        event_name = 'PoolConfigured'
)
select
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:principalOut_::number as principalOut_
from
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
where
    event_name = 'PrincipalOutUpdated'
    and (contract_address in (
        SELECT
            lm_address
        from
            loan_manager_addresses
    )
    or contract_address = '0x7a459f1fb7d257fc62e23aaa8b802e061cec68d7')