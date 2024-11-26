{{
    config(
        materialized = 'table',
        database = 'hashnote',
        schema = 'core',
        snowflake_warehouse = 'HASHNOTE',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('hashnote') }}
