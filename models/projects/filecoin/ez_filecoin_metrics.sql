{{
    config(
        materialized="table",
        snowflake_warehouse="FILECOIN",
        database="filecoin",
        schema="core",
        alias="ez_metrics",
    )
}}

with market_data as (
    {{ get_coingecko_metrics("filecoin") }}
)
, revenue as (
    SELECT
        date,
        total_burn_native
    FROM {{ ref("fact_filecoin_revenue") }}
)
SELECT
    market_data.date,
    revenue.total_burn_native,
    revenue.total_burn_native * market_data.price as total_burn,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume
FROM market_data
LEFT JOIN revenue USING(date)
    