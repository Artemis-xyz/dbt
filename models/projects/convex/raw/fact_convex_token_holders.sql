{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_token_holders'
    )
}}

{{ token_holders('ethereum', '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b', '2021-05-17') }}