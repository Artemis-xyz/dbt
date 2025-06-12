{{
    config(
        materialized="table",
        snowflake_warehouse="AETHIR",
        database="aethir",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


SELECT
    date,
    'arbitrum' as chain,
    compute_revenue as revenue
FROM {{ ref("fact_aethir_compute_revenue") }}    