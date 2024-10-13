{{
    config(
        materialized="table",
        snowflake_warehouse="MAPLE",
    )
}}

-- 380 rows expected, 
with loan_manager_addresses as (
    SELECT
        '0x' || SUBSTR(topics[1], 24+3, 40)::string as lm_address
    FROM
        {{source('ETHEREUM_FLIPSIDE', 'fact_event_logs')}}
    where
        topics[0] = '0x870b352f9e61b22ce039fe5f1976fa831c1e76b68d0f7b86965abb7fad3d8112'
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
    and contract_address in (
        SELECT
            lm_address
        from
            loan_manager_addresses
    ) 
    and contract_address <> '0x7a459f1fb7d257fc62e23aaa8b802e061cec68d7'