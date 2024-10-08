{{
    config(
        materialized="view",
        database="outerlands",
        snowflake_warehouse="OUTERLANDS",
        schema="core",
        alias="ez_metrics"
    )
}}

SELECT date, cumulative_index_value as price FROM {{ ref('fact_outerlands_fundamental_index_performance') }}