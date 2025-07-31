{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        unique_key=["address", "contract_address", "address"]
    )
}}

{{ forward_filled_balance_for_address('solana', 'AyzyikXL9kKs2cwyHsWLEe22aRYAvhbWwFn9TKrgmMx') }}