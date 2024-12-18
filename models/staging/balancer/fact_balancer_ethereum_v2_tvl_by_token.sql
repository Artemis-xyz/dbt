{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
    )
}}

{{ get_single_address_historical_balance_by_token_and_chain('ethereum', '0xBA12222222228d8Ba445958a75a0704d566BF2C8', '2021-04-20') }}