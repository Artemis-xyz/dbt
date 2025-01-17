{{
    config(
        materialized = 'table',
        database = 'franklin_templeton',
        schema = 'core',
        snowflake_warehouse = 'FRANKLIN_TEMPLETON',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('franklin_templeton') }}
