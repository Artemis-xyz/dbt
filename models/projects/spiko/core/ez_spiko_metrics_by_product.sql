{{
    config(
        materialized = 'table',
        database = 'spiko',
        schema = 'core',
        snowflake_warehouse = 'SPIKO',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('spiko') }}
