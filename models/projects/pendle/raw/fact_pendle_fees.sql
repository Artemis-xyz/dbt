{{
    config(
        materialized = 'view'
    )
}}


SELECT
    date
    , chain
    , fees
    , revenue
    , supply_side_fees
FROM {{ref('fact_pendle_fees_silver')}}
where date < current_date()