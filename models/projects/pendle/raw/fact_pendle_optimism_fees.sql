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
    {{ ref('fact_pendle_optimism_fees_silver') }}