{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BALANCER',
        database = 'BALANCER',
        schema = 'raw',
        alias = 'fact_balancer_v2_gnosis_pool_metadata'
    )
}}

{{ get_balancer_v2_pool_metadata('gnosis') }}