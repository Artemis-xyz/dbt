{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_seniorpool_reservefundscollected'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    NULL AS addr,
    decoded_log:amount::number / 1e6 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'ReserveFundsCollected'
    AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'