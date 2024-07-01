{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_automation_upkeep_performed_logs"
    )
}}

select
    'ethereum' as chain
    , contract_address
    , decoded_log
    , tx_hash
    , block_number
    , block_timestamp
    , event_index
    , origin_from_address as tx_from
from ethereum_flipside.core.ez_decoded_event_logs
where topics[0]::string = '0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6' -- UpkeepPerformed
or topics[0]::string = '0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b' -- UpkeepPerformedV2