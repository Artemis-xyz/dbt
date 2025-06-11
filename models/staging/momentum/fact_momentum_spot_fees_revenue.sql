{{config(
    materialized = 'table',
    database = 'momentum'
)}}

SELECT
    date,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS fees_native, 
    SUM(fee_amount_usd) AS fees, 
    SUM(protocol_fee_amount_native) AS foundation_cash_flow_native, 
    SUM(protocol_fee_amount_usd) AS foundation_cash_flow,
    SUM(fee_amount_native) - SUM(protocol_fee_amount_native) AS service_cash_flow_native,
    SUM(fee_amount_usd) - SUM(protocol_fee_amount_usd) AS service_cash_flow
FROM {{ ref('fact_raw_momentum_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5