{{
    config(
        materialized="incremental",
        snowflake_warehouse="ORCA",
    )
}}

{{ forward_filled_address_balances('solana', 'orca', 'treasury')}}