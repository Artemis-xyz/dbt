{{
    config(
        materialized = 'view'
        )
}}

SELECT
    date
    , chain
    , daus
    , txns
FROM
    {{ ref('fact_pendle_arbitrum_daus_txns_silver') }}