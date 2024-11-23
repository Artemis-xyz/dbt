{{
    config(
        materialized = 'table',
        database = 'blackrock',
        schema = 'core',
        snowflake_warehouse = 'BLACKROCK',
        alias = 'ez_metrics_by_chain'
    )
}}

{{ ez_rwa_by_chain('blackrock') }}
