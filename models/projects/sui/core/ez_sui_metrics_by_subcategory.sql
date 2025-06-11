{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_subcategory",
    )
}}
SELECT
    * EXCLUDE date,
    TO_TIMESTAMP_NTZ(date) AS date,
    null AS contract_count,
    null AS real_users
FROM {{ source('PROD_LANDING', 'ez_sui_metrics_by_subcategory') }}