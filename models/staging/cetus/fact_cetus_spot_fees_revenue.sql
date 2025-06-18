{{config(
    materialized = 'table',
    database = 'cetus'
)}}

--To maintain a healthy economic model that is beneficial to the project's sustainable project treasury for its long term development, a certain percentage (20% by default) will be taken from swap fees of every transaction on Cetus as the protocol fee.
-- Source: https://cetus-1.gitbook.io/cetus-docs/protocol-concepts/fees

SELECT
    date,
    transaction_digest,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS fees_native, 
    SUM(fee_amount_usd) AS fees, 
    0.2 * SUM(fee_amount_native) AS foundation_fee_allocation_native, 
    0.2 * SUM(fee_amount_usd) AS foundation_fee_allocation, 
    0.8 * SUM(fee_amount_native) AS service_fee_allocation_native, 
    0.8 * SUM(fee_amount_usd) AS service_fee_allocation
FROM {{ ref('fact_raw_cetus_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5, 6