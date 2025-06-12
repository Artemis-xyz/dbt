{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
        database="braintrust",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with revenue as (
    SELECT
        date,
        burns,
        burns_native
    FROM {{ ref("fact_braintrust_revenue") }}
)
, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between (SELECT min(date) from revenue) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics("braintrust") }}
)

SELECT
    date_spine.date,
    'ethereum' as chain,
    market_metrics.price,
    market_metrics.market_cap,
    market_metrics.fdmc,
    market_metrics.token_turnover_circulating,
    market_metrics.token_turnover_fdv,
    revenue.burns as revenue,
    revenue.burns_native as burns_native
FROM date_spine
LEFT JOIN revenue USING(date)
LEFT JOIN market_metrics USING(date)