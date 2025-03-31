{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
        database="braintrust",
        schema="core",
        alias="ez_metrics",
    )
}}

with market_data as (
    {{ get_coingecko_metrics("braintrust") }}
)
, revenue as (
    SELECT
        date,
        burns
    FROM {{ ref("fact_braintrust_revenue") }}
)
SELECT
    market_data.date,
    revenue.burns,
    market_data.price,
    market_data.market_cap,
    market_data.fdmc,
    market_data.token_turnover_circulating,
    market_data.token_turnover_fdv,
    market_data.token_volume
FROM market_data
LEFT JOIN revenue USING(date)