{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
        database="braintrust",
        schema="core",
        alias="ez_metrics",
    )
}}

with revenue as (
    SELECT
        date,
        revenue
    FROM {{ ref("fact_braintrust_revenue") }}
)
, market_metrics as (
    {{ get_coingecko_metrics("braintrust") }}
)
, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between (SELECT min(date) from market_metrics) and to_date(sysdate())
)
SELECT
    date_spine.date,
    market_metrics.price,
    market_metrics.market_cap,
    market_metrics.fdmc,
    market_metrics.token_turnover_circulating,
    market_metrics.token_turnover_fdv,
    coalesce(revenue.revenue, 0) as revenue
FROM date_spine
LEFT JOIN revenue USING(date)
LEFT JOIN market_metrics USING(date)