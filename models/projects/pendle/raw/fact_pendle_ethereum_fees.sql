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
    {{ ref('fact_pendle_ethereum_fees_silver') }}