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
    SUM(fee_amount_usd) AS fees_usd, 
    SUM(protocol_fee_share_amount_native) AS protocol_fee_share_native, 
    SUM(protocol_fee_share_amount_usd) AS protocol_fee_share_usd
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5