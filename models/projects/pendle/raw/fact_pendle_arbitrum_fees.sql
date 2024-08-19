{{
    config(
        materialized = 'view'
        )
}}

SELECT
    date
    , chain
    , fees
FROM
    {{ ref('fact_pendle_arbitrum_fees_silver') }}