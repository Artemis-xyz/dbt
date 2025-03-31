{{
    config(
        materialized="table",
        snowflake_warehouse="GLOW",
        database="glow",
        schema="core",
        alias="ez_metrics",
    )
}}

with revenue as (
    SELECT
        date,
        revenue
    FROM {{ ref("fact_glow_compute_revenue") }}
)
,  date_spine as (
    SELECT date
    FROM {{ ref("dim_date_spine") }}
    WHERE date between (SELECT MIN(date) from revenue) and to_date(sysdate())
)

SELECT
    date_spine.date,
    revenue.revenue
FROM date_spine
LEFT JOIN revenue USING(date)
