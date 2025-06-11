{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date,
    transaction_digest,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS fees_native, 
    SUM(fee_amount_usd) AS fees, 
    SUM(protocol_fee_share_amount_native) AS foundation_fee_allocation_native, 
    SUM(protocol_fee_share_amount_usd) AS foundation_fee_allocation, 
    SUM(fee_amount_native) - SUM(protocol_fee_share_amount_native) AS service_fee_allocation_native, 
    SUM(fee_amount_usd) - SUM(protocol_fee_share_amount_usd) AS service_fee_allocation
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5, 6