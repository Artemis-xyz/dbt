{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

SELECT
    date,
    transaction_digest,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS ecosystem_revenue_native, 
    SUM(fee_amount_usd) AS ecosystem_revenue, 
    SUM(protocol_fee_share_amount_native) AS foundation_cash_flow_native,
    SUM(protocol_fee_share_amount_usd) AS foundation_cash_flow, 
    SUM(fee_amount_native) - SUM(protocol_fee_share_amount_native) AS service_cash_flow_native,
    SUM(fee_amount_usd) - SUM(protocol_fee_share_amount_usd) AS service_cash_flow
FROM {{ ref('fact_raw_aftermath_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5, 6