{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_migratedtranchedpool_reservefundscollected'
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
    AND contract_address IN (SELECT migratedtranchepool_address FROM {{ref('dim_migratedtranchepools_addresses')}})