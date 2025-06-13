{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'SYRUP',
        database = 'SYRUP',
        schema = 'core',
        alias = 'ez_metrics_by_chain'
    )
}}
SELECT 
    *
FROM {{ ref('ez_maple_metrics_by_chain') }}
