{{
    config(
        materialized='table',
        snowflake_warehouse='ANALYTICS_XL'
    )
}}

{{ forward_filled_balance_for_address('ethereum', '0x4f30A9D41B80ecC5B94306AB4364951AE3170210')}}