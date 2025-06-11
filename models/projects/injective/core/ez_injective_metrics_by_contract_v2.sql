{{
    config(
        materialized="table",
        snowflake_warehouse="INJECTIVE",
        database="injective",
        schema="core",
        alias="ez_metrics_by_contract_v2",
    )
}}
SELECT
    * EXCLUDE date,
    TO_TIMESTAMP_NTZ(date) AS date,
    null AS real_users
FROM {{ source('PROD_LANDING', 'ez_injective_metrics_by_contract_v2') }}