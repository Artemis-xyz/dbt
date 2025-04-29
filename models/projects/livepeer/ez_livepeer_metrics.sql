{{
    config(
        materialized="table",
        snowflake_warehouse="LIVEPEER",
        database="livepeer",
        schema="core",
        alias="ez_metrics",
    )
}}

with market_data as (
    {{ get_coingecko_metrics("livepeer") }}
)
, revenue as (
    SELECT
        date,
        fees
    FROM {{ ref("fact_livepeer_revenue") }}
)
SELECT
    market_data.date,
    revenue.fees,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume
FROM market_data
LEFT JOIN revenue USING(date)