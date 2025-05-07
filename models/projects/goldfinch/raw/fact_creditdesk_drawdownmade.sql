{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_creditdesk_drawdownmade'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    'drawdown' AS tx_type,
    decoded_log:borrower::string AS addr,
    decoded_log:drawdownAmount::number / 1e6 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'DrawdownMade'
    AND LOWER(contract_address) = '0xd52dc1615c843c30f2e4668e101c0938e6007220'