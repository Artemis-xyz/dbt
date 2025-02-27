{{
    config(
        materialized = 'table',
        database = 'spiko',
        schema = 'core',
        snowflake_warehouse = 'SPIKO',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('spiko') }}
