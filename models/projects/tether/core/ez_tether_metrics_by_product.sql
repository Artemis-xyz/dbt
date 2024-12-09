{{
    config(
        materialized = 'table',
        database = 'tether',
        schema = 'core',
        snowflake_warehouse = 'TETHER',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('tether') }}
