{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'GOLDFINCH',
        database = 'goldfinch',
        schema = 'raw',
        alias = 'fact_migratedtranchedpool_withdrawalmade'
    )
}}

SELECT 
    block_timestamp,
    tx_hash,
    contract_address,
    decoded_log:owner::string AS addr,
    decoded_log:interestWithdrawn::number / 1e6 AS interest_withdrawn,
    decoded_log:principalWithdrawn::number / 1e6 AS principal_withdrawn
FROM 
    ethereum_flipside.core.ez_decoded_event_logs
WHERE 
    event_name = 'WithdrawalMade'
    AND (contract_address IN (SELECT migratedtranchepool_address FROM {{ref('dim_migratedtranchepools_addresses')}})
or contract_address = lower('0xd43a4f3041069c6178b99d55295b00d0db955bb5'))