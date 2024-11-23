{{
    config(
        materialized = 'table',
        database = 'ondo',
        schema = 'core',
        snowflake_warehouse = 'ONDO',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('ondo') }}