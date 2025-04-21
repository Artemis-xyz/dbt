
{{
    config(
        materialized="incremental",
        unique_key=["address", "contract_address", "block_timestamp"],
        snowflake_warehouse="SEI",
    )
}}

{{ evm_address_balances("sei") }}