{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date,
    SUM((COALESCE(fee_amount, 0) * COALESCE(price_fee, 0))) AS fee_amount
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1