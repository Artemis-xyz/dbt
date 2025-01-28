{{
    config(
        materialized="table",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}
SELECT
    * EXCLUDE date,
    TO_TIMESTAMP_NTZ(date) AS date
FROM {{ source('PROD_LANDING', 'ez_stellar_metrics_by_application') }}