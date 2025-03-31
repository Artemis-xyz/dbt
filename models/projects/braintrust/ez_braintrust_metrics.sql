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
        burns
    FROM {{ ref("fact_braintrust_revenue") }}
)
, date_spine as (
    select date
    from {{ ref("dim_date_spine") }}
    where date between (SELECT min(date) from revenue) and to_date(sysdate())
)

SELECT
    date_spine.date,
    revenue.burns
FROM date_spine
LEFT JOIN revenue USING(date)