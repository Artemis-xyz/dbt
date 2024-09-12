{{
    config(
        materialized="view",
        database="outerlands",
        snowflake_warehouse="OUTERLANDS",
        schema="core",
        alias="ez_metrics"
    )
}}

SELECT * FROM {{ ref('fact_outerlands_index_full_universe') }}