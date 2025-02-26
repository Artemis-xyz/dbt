{{
    config(
        materialized = 'table',
        database = 'wisdomtree',
        schema = 'core',
        snowflake_warehouse = 'WISDOMTREE',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('wisdomtree') }}
