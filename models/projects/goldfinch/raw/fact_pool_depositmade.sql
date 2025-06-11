{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_pool_depositmade'
    )
}}


SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:capitalProvider::string AS addr,
    decoded_log:amount::number / 1e6 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'DepositMade'
    AND contract_address = lower('0xB01b315e32D1D9B5CE93e296D483e1f0aAD39E75')