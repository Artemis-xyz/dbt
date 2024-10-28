{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE',
        database='MAPLE',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

SELECT 1