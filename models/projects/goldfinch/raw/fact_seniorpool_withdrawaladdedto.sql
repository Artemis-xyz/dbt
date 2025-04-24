{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_seniorpool_withdrawaladdedto'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:operator::string AS addr,
    decoded_log:fiduRequested::number / 1e18 AS amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'WithdrawalAddedTo'
    AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'  