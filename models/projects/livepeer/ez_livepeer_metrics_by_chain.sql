{{
    config(
        materialized="table",
        snowflake_warehouse="LIVEPEER",
        database="livepeer",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

SELECT
    date,
    'arbitrum' as chain,
    fees
FROM {{ ref("fact_livepeer_revenue") }}
