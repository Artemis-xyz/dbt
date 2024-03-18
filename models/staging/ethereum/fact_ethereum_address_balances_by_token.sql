{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="BALANCES_LG",
    )
}}

{{ address_balances_with_flipside_ez("ethereum") }}
