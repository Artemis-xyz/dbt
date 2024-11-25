{{
    config(
        materialized = 'table',
        database = 'franklin_templeton',
        schema = 'core',
        snowflake_warehouse = 'FRANKLIN_TEMPLETON',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('franklin_templeton') }}
