{{
    config(
        materialized="table",
        snowflake_warehouse="AETHIR",
        database="aethir",
        schema="core",
        alias="ez_metrics",
    )
}}

with market_data as (
    {{ get_coingecko_metrics("aethir") }}
)
, revenue as (
    SELECT
        date,
        compute_revenue
    FROM {{ ref("fact_aethir_compute_revenue") }}    
)
SELECT
    market_data.date,
    revenue.compute_revenue as revenue,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume
FROM market_data
LEFT JOIN revenue USING(date)