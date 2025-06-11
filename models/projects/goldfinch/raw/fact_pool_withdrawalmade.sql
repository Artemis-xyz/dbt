{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_pool_withdrawalmade'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:capitalProvider::string AS addr,
    decoded_log:userAmount::number / 1e6 AS user_amount,
    decoded_log:reserveAmount::number / 1e6 AS reserve_amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'WithdrawalMade'
    AND contract_address = lower('0xB01b315e32D1D9B5CE93e296D483e1f0aAD39E75')