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
    {{ ref('fact_pendle_bsc_fees_silver') }}