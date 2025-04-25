{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

{{ forward_filled_address_balances('solana', 'orca', 'treasury')}}