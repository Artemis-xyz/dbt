{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_category_v2",
    )
}}
SELECT
    * EXCLUDE date,
    TO_TIMESTAMP_NTZ(date) AS date
FROM {{ source('PROD_LANDING', 'ez_sui_metrics_by_category_v2') }}