{{
    config(
        materialized = 'table',
        database = 'tether',
        schema = 'core',
        snowflake_warehouse = 'TETHER',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('tether') }}
