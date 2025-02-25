{{
    config(
        materialized = 'table',
        database = 'wisdomtree',
        schema = 'core',
        snowflake_warehouse = 'WISDOMTREE',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('wisdomtree') }}
