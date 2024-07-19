{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
        database="injective",
        schema="core",
        alias="ez_metrics_by_application",
    )
}}
SELECT
    * EXCLUDE date,
    DATEADD('day', 1, DATE_TRUNC('day', TO_TIMESTAMP_NTZ(date))) AS date
FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_application') }}