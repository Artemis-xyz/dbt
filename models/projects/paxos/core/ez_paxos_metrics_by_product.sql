{{
    config(
        materialized = 'table',
        database = 'paxos',
        schema = 'core',
        snowflake_warehouse = 'PAXOS',
        alias = 'ez_metrics_by_product'
    )
}}

{{ ez_rwa_by_product('Paxos') }}
