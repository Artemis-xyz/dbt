{{
    config(
        materialized="incremental",
        snowflake_warehouse="ORCA",
        unique_key="unique_id"
    )
}}

{{ forward_filled_address_balances('solana', 'orca', 'treasury')}}