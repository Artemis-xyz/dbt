{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM"
    )
}}

SELECT
    block_timestamp, 
    'base' AS chain, 
    contract_address, 
    tx_hash, 
    decoded_log:_paidToOptimism::number/1e18 AS optimismFeeShare
FROM base_flipside.core.ez_decoded_event_logs
WHERE event_name = 'FeesDisbursed'

UNION ALL 

SELECT 
    block_timestamp, 
    'ethereum' AS chain, 
    contract_address, 
    tx_hash, 
    decoded_log:amount::number/1e18 AS optimismFeeShare
FROM ethereum_flipside.core.ez_decoded_event_logs
WHERE LOWER(decoded_log:to::string) IN (LOWER('0xa3d596eafab6b13ab18d40fae1a962700c84adea'), 
                                        LOWER('0x391716d440c151c42cdf1c95c1d83a5427bca52c')
                                        )
    AND event_name = 'ETHBridgeFinalized'

UNION ALL

SELECT
    block_timestamp, 
    'unichain' AS chain, 
    contract_address, 
    transaction_hash, 
    decoded_log:reflectionFee::number/1e18 AS optimismFeeShare  
FROM PC_DBT_DB.PROD.FACT_UNICHAIN_DECODED_EVENTS
WHERE event_name = 'FeesDistributed'