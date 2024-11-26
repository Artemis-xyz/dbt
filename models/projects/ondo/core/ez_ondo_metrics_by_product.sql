{{
    config(
        materialized = 'table',
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('ondo') }}
