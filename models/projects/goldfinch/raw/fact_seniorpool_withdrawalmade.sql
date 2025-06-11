{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_seniorpool_withdrawalmade'
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
    AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'