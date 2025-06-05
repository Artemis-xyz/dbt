{{
    config(
        materialized="table",
        snowflake_warehouse="SUI",
        database="sui",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}
SELECT
    * EXCLUDE date,
    TO_TIMESTAMP_NTZ(date) AS date,
    null AS real_users
FROM {{ source('PROD_LANDING', 'ez_sui_metrics_by_contract_v2') }}