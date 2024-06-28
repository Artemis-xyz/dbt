{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_vrf_v1_random_request_logs",
    )
}}

select
    block_timestamp,
    tx_hash,
    origin_from_address as tx_from,
    CAST(GET_PATH(decoded_log,'fee') as double) as fee,
    GET_PATH(decoded_log, 'requestID') as request_id
from ethereum_flipside.core.ez_decoded_event_logs
where event_name = 'RandomnessRequest'
and contract_address = '0xf0d54349addcf704f77ae15b96510dea15cb7952'