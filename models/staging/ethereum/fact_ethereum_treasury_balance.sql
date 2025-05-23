{{
    config(
        materialized="table",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

{{ forward_filled_balance_for_address('ethereum', '0xde0B295669a9FD93d5F28D9Ec85E40f4cb697BAe') }}