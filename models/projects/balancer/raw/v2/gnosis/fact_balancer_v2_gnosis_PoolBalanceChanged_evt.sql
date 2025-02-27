{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_v2_gnosis_PoolBalanceChanged_evt'
    )
}}

{{ get_balancer_v2_PoolBalanceChanged('gnosis') }}