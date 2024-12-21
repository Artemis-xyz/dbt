{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

{{ token_holders('ethereum', '0xba100000625a3754423978a60c9317c58a424e3D', '2020-06-20') }}