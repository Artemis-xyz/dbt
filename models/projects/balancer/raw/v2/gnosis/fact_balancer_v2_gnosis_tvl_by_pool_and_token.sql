{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_v2_gnosis_tvl_by_pool_and_token'
    )
}}

{{ get_balancer_v2_tvl_by_pool_and_token('gnosis') }}