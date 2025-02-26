{{
    config(
        materialized = 'table',
        database = 'superstate',
        schema = 'core',
        snowflake_warehouse = 'SUPERSTATE',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('superstate') }}

