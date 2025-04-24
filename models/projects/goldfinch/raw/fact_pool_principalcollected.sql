{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_pool_principalcollected'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:payer::string AS addr,
    decoded_log:amount::number / 1e6 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'PrincipalCollected'
    AND LOWER(contract_address) = '0xb01b315e32d1d9b5ce93e296d483e1f0aad39e75'
