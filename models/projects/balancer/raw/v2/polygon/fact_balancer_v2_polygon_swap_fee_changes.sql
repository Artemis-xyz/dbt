{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'BALANCER',
        database = 'BALANCER',
        schema = 'raw',
        alias = 'fact_balancer_v2_polygon_swap_fee_changes'
    )
}}

{{ get_balancer_v2_swap_fee_changes('polygon') }}