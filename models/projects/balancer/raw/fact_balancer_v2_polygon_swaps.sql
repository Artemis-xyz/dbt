{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'MEDIUM',
        database = 'BALANCER',
        schema = 'raw',
        alias = 'fact_balancer_v2_polygon_swaps'
    )
}}

{{ get_balancer_v2_swaps('polygon') }}