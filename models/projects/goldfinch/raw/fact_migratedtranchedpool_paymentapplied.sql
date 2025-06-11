{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_migratedtranchedpool_paymentapplied'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:payer::string AS addr,
    decoded_log:interestAmount::number / 1e6 AS interest_amount,
    decoded_log:principalAmount::number / 1e6 AS principal_amount,
    decoded_log:reserveAmount::number / 1e6 AS reserve_amount
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'PaymentApplied'
    AND contract_address IN (SELECT migratedtranchepool_address FROM {{ref('dim_migratedtranchepools_addresses')}})