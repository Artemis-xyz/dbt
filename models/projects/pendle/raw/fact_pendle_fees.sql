{{
    config(
        materialized="view",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_fees",
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