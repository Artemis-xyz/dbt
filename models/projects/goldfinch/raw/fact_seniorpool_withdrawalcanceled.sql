{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_seniorpool_withdrawalcanceled'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:operator::string AS addr,
    decoded_log:fiduCanceled::number / 1e18 AS fidu_canceled,
    decoded_log:reserveFidu::number / 1e18 AS reserve_fidu
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'WithdrawalCanceled'
    AND LOWER(contract_address) = '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822'