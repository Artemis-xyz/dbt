{{ config(materialized="table") }}

with
float_revenue as (
    SELECT 
        extraction_date::date AS date,
        AVG(PARSE_JSON(source_json):value::float) as daily_avg_float_revenue
    FROM {{source("PROD_LANDING", "raw_drift_float_revenue")}} 
    GROUP BY 1
),
lending_revenue as (
    SELECT 
        extraction_date::date AS date,
        AVG(PARSE_JSON(source_json):value::float) as daily_avg_lending_revenue
    FROM {{source("PROD_LANDING", "raw_drift_borrow_lending_revenue")}}
    GROUP BY 1
)
SELECT 
    coalesce(float_revenue.date, lending_revenue.date) as date,
    daily_avg_float_revenue,
    daily_avg_lending_revenue
FROM float_revenue
LEFT JOIN lending_revenue 
    ON float_revenue.date = lending_revenue.date
