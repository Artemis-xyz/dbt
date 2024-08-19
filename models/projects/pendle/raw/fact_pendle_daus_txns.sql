{{
    config(
        materialized = 'view'
    )
}}


SELECT
    date
    , chain
    , daus
    , daily_txns
FROM {{ref('fact_pendle_daus_txns_silver')}}
where date < current_date()