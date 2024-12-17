{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

SELECT 1;