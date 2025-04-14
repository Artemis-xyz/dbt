{{
    config(
        materialized="table",
        snowflake_warehouse="BRAINTRUST",
        database="braintrust",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}
SELECT
    date,
    burns
FROM {{ ref("fact_braintrust_revenue") }}
