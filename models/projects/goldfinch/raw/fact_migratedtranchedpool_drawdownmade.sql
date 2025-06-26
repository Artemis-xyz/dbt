{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_migratedtranchedpool_drawdownmade'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:borrower::string AS addr,
    decoded_log:amount::number / 1e6 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'DrawdownMade'
    AND contract_address IN (SELECT migratedtranchepool_address FROM {{ref('dim_migratedtranchepools_addresses')}})