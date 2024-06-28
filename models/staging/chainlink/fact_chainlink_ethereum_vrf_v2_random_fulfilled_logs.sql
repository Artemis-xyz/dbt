{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_v2_random_fulfilled_logs",
    )
}}

select
    block_timestamp,
    tx_hash,
    event_index,
    GET_PATH(decoded_log, 'payment') as payment
from ethereum_flipside.core.ez_decoded_event_logs
where event_name = 'RandomWordsFulfilled'
and contract_address = '0x271682deb8c4e0901d1a1550ad2e64d568e69909'