{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

{{ token_holders('ethereum', '0x33349b282065b0284d756f0577fb39c158f935e6', '2021-04-20') }}