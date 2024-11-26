{{
    config(
        materialized = 'table',
        database = 'openeden',
        schema = 'core',
        snowflake_warehouse = 'OPENEDEN',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('openeden') }}