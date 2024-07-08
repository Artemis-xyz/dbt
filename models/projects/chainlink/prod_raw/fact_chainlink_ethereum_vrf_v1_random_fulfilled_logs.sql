{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_v1_random_fulfilled_logs",
    )
}}

select
    block_timestamp,
    tx_hash,
    GET_PATH(decoded_log, 'requestId') as request_id
from ethereum_flipside.core.ez_decoded_event_logs
where event_name = 'RandomnessRequestFulfilled'
and contract_address = '0xf0d54349addcf704f77ae15b96510dea15cb7952'