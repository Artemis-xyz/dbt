{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_token_holders'
    )
}}

{{ token_holders('ethereum', '0x6DEA81C8171D0bA574754EF6F8b412F2Ed88c54D', '2021-04-04') }}